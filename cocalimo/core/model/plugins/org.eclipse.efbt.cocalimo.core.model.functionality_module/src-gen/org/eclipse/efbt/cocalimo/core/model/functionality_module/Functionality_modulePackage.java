/**
 */
package org.eclipse.efbt.cocalimo.core.model.functionality_module;

import org.eclipse.efbt.cocalimo.core.model.module_management.Module_managementPackage;

import org.eclipse.efbt.cocalimo.core.model.requirements_text.Requirements_textPackage;

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
 * The functionality_module package describes FunctionalityModules which are sued to describe the functional breakdown and sceanrios of behaviour understood by a domain expert.
 * <!-- end-model-doc -->
 * @see org.eclipse.efbt.cocalimo.core.model.functionality_module.Functionality_moduleFactory
 * @model kind="package"
 *        annotation="license license='Copyright (c) 2019 Bird Software Solutions Ltd\n All rights reserved. This file and the accompanying materials are made available under the terms of the Eclipse Public License v2.0 which accompanies this distribution, and is available at http://www.eclipse.org/legal/epl-v20.html\n\nSPDX-License-Identifier: EPL-2.0 '"
 * @generated
 */
public interface Functionality_modulePackage extends EPackage {
	/**
	 * The package name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNAME = "functionality_module";

	/**
	 * The package namespace URI.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNS_URI = "http://www.eclipse.org/efbt/functionality_module";

	/**
	 * The package namespace name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNS_PREFIX = "functionality_module";

	/**
	 * The singleton instance of the package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	Functionality_modulePackage eINSTANCE = org.eclipse.efbt.cocalimo.core.model.functionality_module.impl.Functionality_modulePackageImpl.init();

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.core.model.functionality_module.impl.FunctionalityModuleImpl <em>Functionality Module</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.core.model.functionality_module.impl.FunctionalityModuleImpl
	 * @see org.eclipse.efbt.cocalimo.core.model.functionality_module.impl.Functionality_modulePackageImpl#getFunctionalityModule()
	 * @generated
	 */
	int FUNCTIONALITY_MODULE = 8;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FUNCTIONALITY_MODULE__NAME = 0;

	/**
	 * The feature id for the '<em><b>Used Module</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FUNCTIONALITY_MODULE__USED_MODULE = 1;

	/**
	 * The feature id for the '<em><b>Scenario</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FUNCTIONALITY_MODULE__SCENARIO = 2;

	/**
	 * The feature id for the '<em><b>Scenario Set</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FUNCTIONALITY_MODULE__SCENARIO_SET = 3;

	/**
	 * The feature id for the '<em><b>Used In Subset</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FUNCTIONALITY_MODULE__USED_IN_SUBSET = 4;

	/**
	 * The number of structural features of the '<em>Functionality Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FUNCTIONALITY_MODULE_FEATURE_COUNT = 5;

	/**
	 * The number of operations of the '<em>Functionality Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FUNCTIONALITY_MODULE_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.core.model.functionality_module.impl.LeafFunctionalityModuleImpl <em>Leaf Functionality Module</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.core.model.functionality_module.impl.LeafFunctionalityModuleImpl
	 * @see org.eclipse.efbt.cocalimo.core.model.functionality_module.impl.Functionality_modulePackageImpl#getLeafFunctionalityModule()
	 * @generated
	 */
	int LEAF_FUNCTIONALITY_MODULE = 1;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int LEAF_FUNCTIONALITY_MODULE__NAME = FUNCTIONALITY_MODULE__NAME;

	/**
	 * The feature id for the '<em><b>Used Module</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int LEAF_FUNCTIONALITY_MODULE__USED_MODULE = FUNCTIONALITY_MODULE__USED_MODULE;

	/**
	 * The feature id for the '<em><b>Scenario</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int LEAF_FUNCTIONALITY_MODULE__SCENARIO = FUNCTIONALITY_MODULE__SCENARIO;

	/**
	 * The feature id for the '<em><b>Scenario Set</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int LEAF_FUNCTIONALITY_MODULE__SCENARIO_SET = FUNCTIONALITY_MODULE__SCENARIO_SET;

	/**
	 * The feature id for the '<em><b>Used In Subset</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int LEAF_FUNCTIONALITY_MODULE__USED_IN_SUBSET = FUNCTIONALITY_MODULE__USED_IN_SUBSET;

	/**
	 * The number of structural features of the '<em>Leaf Functionality Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int LEAF_FUNCTIONALITY_MODULE_FEATURE_COUNT = FUNCTIONALITY_MODULE_FEATURE_COUNT + 0;

	/**
	 * The number of operations of the '<em>Leaf Functionality Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int LEAF_FUNCTIONALITY_MODULE_OPERATION_COUNT = FUNCTIONALITY_MODULE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.core.model.functionality_module.impl.DataProcessingFunctionalityModuleImpl <em>Data Processing Functionality Module</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.core.model.functionality_module.impl.DataProcessingFunctionalityModuleImpl
	 * @see org.eclipse.efbt.cocalimo.core.model.functionality_module.impl.Functionality_modulePackageImpl#getDataProcessingFunctionalityModule()
	 * @generated
	 */
	int DATA_PROCESSING_FUNCTIONALITY_MODULE = 0;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DATA_PROCESSING_FUNCTIONALITY_MODULE__NAME = LEAF_FUNCTIONALITY_MODULE__NAME;

	/**
	 * The feature id for the '<em><b>Used Module</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DATA_PROCESSING_FUNCTIONALITY_MODULE__USED_MODULE = LEAF_FUNCTIONALITY_MODULE__USED_MODULE;

	/**
	 * The feature id for the '<em><b>Scenario</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DATA_PROCESSING_FUNCTIONALITY_MODULE__SCENARIO = LEAF_FUNCTIONALITY_MODULE__SCENARIO;

	/**
	 * The feature id for the '<em><b>Scenario Set</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DATA_PROCESSING_FUNCTIONALITY_MODULE__SCENARIO_SET = LEAF_FUNCTIONALITY_MODULE__SCENARIO_SET;

	/**
	 * The feature id for the '<em><b>Used In Subset</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DATA_PROCESSING_FUNCTIONALITY_MODULE__USED_IN_SUBSET = LEAF_FUNCTIONALITY_MODULE__USED_IN_SUBSET;

	/**
	 * The feature id for the '<em><b>Enriched Attribute</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DATA_PROCESSING_FUNCTIONALITY_MODULE__ENRICHED_ATTRIBUTE = LEAF_FUNCTIONALITY_MODULE_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Data Processing Functionality Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DATA_PROCESSING_FUNCTIONALITY_MODULE_FEATURE_COUNT = LEAF_FUNCTIONALITY_MODULE_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Data Processing Functionality Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DATA_PROCESSING_FUNCTIONALITY_MODULE_OPERATION_COUNT = LEAF_FUNCTIONALITY_MODULE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.core.model.functionality_module.impl.FunctionalityModuleSetImpl <em>Functionality Module Set</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.core.model.functionality_module.impl.FunctionalityModuleSetImpl
	 * @see org.eclipse.efbt.cocalimo.core.model.functionality_module.impl.Functionality_modulePackageImpl#getFunctionalityModuleSet()
	 * @generated
	 */
	int FUNCTIONALITY_MODULE_SET = 5;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FUNCTIONALITY_MODULE_SET__NAME = FUNCTIONALITY_MODULE__NAME;

	/**
	 * The feature id for the '<em><b>Used Module</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FUNCTIONALITY_MODULE_SET__USED_MODULE = FUNCTIONALITY_MODULE__USED_MODULE;

	/**
	 * The feature id for the '<em><b>Scenario</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FUNCTIONALITY_MODULE_SET__SCENARIO = FUNCTIONALITY_MODULE__SCENARIO;

	/**
	 * The feature id for the '<em><b>Scenario Set</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FUNCTIONALITY_MODULE_SET__SCENARIO_SET = FUNCTIONALITY_MODULE__SCENARIO_SET;

	/**
	 * The feature id for the '<em><b>Used In Subset</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FUNCTIONALITY_MODULE_SET__USED_IN_SUBSET = FUNCTIONALITY_MODULE__USED_IN_SUBSET;

	/**
	 * The feature id for the '<em><b>Sub Modules</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FUNCTIONALITY_MODULE_SET__SUB_MODULES = FUNCTIONALITY_MODULE_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Functionality Module Set</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FUNCTIONALITY_MODULE_SET_FEATURE_COUNT = FUNCTIONALITY_MODULE_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Functionality Module Set</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FUNCTIONALITY_MODULE_SET_OPERATION_COUNT = FUNCTIONALITY_MODULE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.core.model.functionality_module.impl.ParallelFunctionalityModuleImpl <em>Parallel Functionality Module</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.core.model.functionality_module.impl.ParallelFunctionalityModuleImpl
	 * @see org.eclipse.efbt.cocalimo.core.model.functionality_module.impl.Functionality_modulePackageImpl#getParallelFunctionalityModule()
	 * @generated
	 */
	int PARALLEL_FUNCTIONALITY_MODULE = 2;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PARALLEL_FUNCTIONALITY_MODULE__NAME = FUNCTIONALITY_MODULE_SET__NAME;

	/**
	 * The feature id for the '<em><b>Used Module</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PARALLEL_FUNCTIONALITY_MODULE__USED_MODULE = FUNCTIONALITY_MODULE_SET__USED_MODULE;

	/**
	 * The feature id for the '<em><b>Scenario</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PARALLEL_FUNCTIONALITY_MODULE__SCENARIO = FUNCTIONALITY_MODULE_SET__SCENARIO;

	/**
	 * The feature id for the '<em><b>Scenario Set</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PARALLEL_FUNCTIONALITY_MODULE__SCENARIO_SET = FUNCTIONALITY_MODULE_SET__SCENARIO_SET;

	/**
	 * The feature id for the '<em><b>Used In Subset</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PARALLEL_FUNCTIONALITY_MODULE__USED_IN_SUBSET = FUNCTIONALITY_MODULE_SET__USED_IN_SUBSET;

	/**
	 * The feature id for the '<em><b>Sub Modules</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PARALLEL_FUNCTIONALITY_MODULE__SUB_MODULES = FUNCTIONALITY_MODULE_SET__SUB_MODULES;

	/**
	 * The number of structural features of the '<em>Parallel Functionality Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PARALLEL_FUNCTIONALITY_MODULE_FEATURE_COUNT = FUNCTIONALITY_MODULE_SET_FEATURE_COUNT + 0;

	/**
	 * The number of operations of the '<em>Parallel Functionality Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PARALLEL_FUNCTIONALITY_MODULE_OPERATION_COUNT = FUNCTIONALITY_MODULE_SET_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.core.model.functionality_module.impl.ScenarioSetFunctionalityModuleImpl <em>Scenario Set Functionality Module</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.core.model.functionality_module.impl.ScenarioSetFunctionalityModuleImpl
	 * @see org.eclipse.efbt.cocalimo.core.model.functionality_module.impl.Functionality_modulePackageImpl#getScenarioSetFunctionalityModule()
	 * @generated
	 */
	int SCENARIO_SET_FUNCTIONALITY_MODULE = 3;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SCENARIO_SET_FUNCTIONALITY_MODULE__NAME = FUNCTIONALITY_MODULE_SET__NAME;

	/**
	 * The feature id for the '<em><b>Used Module</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SCENARIO_SET_FUNCTIONALITY_MODULE__USED_MODULE = FUNCTIONALITY_MODULE_SET__USED_MODULE;

	/**
	 * The feature id for the '<em><b>Scenario</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SCENARIO_SET_FUNCTIONALITY_MODULE__SCENARIO = FUNCTIONALITY_MODULE_SET__SCENARIO;

	/**
	 * The feature id for the '<em><b>Scenario Set</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SCENARIO_SET_FUNCTIONALITY_MODULE__SCENARIO_SET = FUNCTIONALITY_MODULE_SET__SCENARIO_SET;

	/**
	 * The feature id for the '<em><b>Used In Subset</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SCENARIO_SET_FUNCTIONALITY_MODULE__USED_IN_SUBSET = FUNCTIONALITY_MODULE_SET__USED_IN_SUBSET;

	/**
	 * The feature id for the '<em><b>Sub Modules</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SCENARIO_SET_FUNCTIONALITY_MODULE__SUB_MODULES = FUNCTIONALITY_MODULE_SET__SUB_MODULES;

	/**
	 * The feature id for the '<em><b>Scenario Set Choice</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SCENARIO_SET_FUNCTIONALITY_MODULE__SCENARIO_SET_CHOICE = FUNCTIONALITY_MODULE_SET_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Scenario Set Functionality Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SCENARIO_SET_FUNCTIONALITY_MODULE_FEATURE_COUNT = FUNCTIONALITY_MODULE_SET_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Scenario Set Functionality Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SCENARIO_SET_FUNCTIONALITY_MODULE_OPERATION_COUNT = FUNCTIONALITY_MODULE_SET_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.core.model.functionality_module.impl.SerialFunctionalityModuleImpl <em>Serial Functionality Module</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.core.model.functionality_module.impl.SerialFunctionalityModuleImpl
	 * @see org.eclipse.efbt.cocalimo.core.model.functionality_module.impl.Functionality_modulePackageImpl#getSerialFunctionalityModule()
	 * @generated
	 */
	int SERIAL_FUNCTIONALITY_MODULE = 4;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SERIAL_FUNCTIONALITY_MODULE__NAME = FUNCTIONALITY_MODULE_SET__NAME;

	/**
	 * The feature id for the '<em><b>Used Module</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SERIAL_FUNCTIONALITY_MODULE__USED_MODULE = FUNCTIONALITY_MODULE_SET__USED_MODULE;

	/**
	 * The feature id for the '<em><b>Scenario</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SERIAL_FUNCTIONALITY_MODULE__SCENARIO = FUNCTIONALITY_MODULE_SET__SCENARIO;

	/**
	 * The feature id for the '<em><b>Scenario Set</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SERIAL_FUNCTIONALITY_MODULE__SCENARIO_SET = FUNCTIONALITY_MODULE_SET__SCENARIO_SET;

	/**
	 * The feature id for the '<em><b>Used In Subset</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SERIAL_FUNCTIONALITY_MODULE__USED_IN_SUBSET = FUNCTIONALITY_MODULE_SET__USED_IN_SUBSET;

	/**
	 * The feature id for the '<em><b>Sub Modules</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SERIAL_FUNCTIONALITY_MODULE__SUB_MODULES = FUNCTIONALITY_MODULE_SET__SUB_MODULES;

	/**
	 * The number of structural features of the '<em>Serial Functionality Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SERIAL_FUNCTIONALITY_MODULE_FEATURE_COUNT = FUNCTIONALITY_MODULE_SET_FEATURE_COUNT + 0;

	/**
	 * The number of operations of the '<em>Serial Functionality Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SERIAL_FUNCTIONALITY_MODULE_OPERATION_COUNT = FUNCTIONALITY_MODULE_SET_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.core.model.functionality_module.impl.ReadDataSourceFunctionalityModuleImpl <em>Read Data Source Functionality Module</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.core.model.functionality_module.impl.ReadDataSourceFunctionalityModuleImpl
	 * @see org.eclipse.efbt.cocalimo.core.model.functionality_module.impl.Functionality_modulePackageImpl#getReadDataSourceFunctionalityModule()
	 * @generated
	 */
	int READ_DATA_SOURCE_FUNCTIONALITY_MODULE = 6;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int READ_DATA_SOURCE_FUNCTIONALITY_MODULE__NAME = LEAF_FUNCTIONALITY_MODULE__NAME;

	/**
	 * The feature id for the '<em><b>Used Module</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int READ_DATA_SOURCE_FUNCTIONALITY_MODULE__USED_MODULE = LEAF_FUNCTIONALITY_MODULE__USED_MODULE;

	/**
	 * The feature id for the '<em><b>Scenario</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int READ_DATA_SOURCE_FUNCTIONALITY_MODULE__SCENARIO = LEAF_FUNCTIONALITY_MODULE__SCENARIO;

	/**
	 * The feature id for the '<em><b>Scenario Set</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int READ_DATA_SOURCE_FUNCTIONALITY_MODULE__SCENARIO_SET = LEAF_FUNCTIONALITY_MODULE__SCENARIO_SET;

	/**
	 * The feature id for the '<em><b>Used In Subset</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int READ_DATA_SOURCE_FUNCTIONALITY_MODULE__USED_IN_SUBSET = LEAF_FUNCTIONALITY_MODULE__USED_IN_SUBSET;

	/**
	 * The feature id for the '<em><b>Entity</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int READ_DATA_SOURCE_FUNCTIONALITY_MODULE__ENTITY = LEAF_FUNCTIONALITY_MODULE_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Read Data Source Functionality Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int READ_DATA_SOURCE_FUNCTIONALITY_MODULE_FEATURE_COUNT = LEAF_FUNCTIONALITY_MODULE_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Read Data Source Functionality Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int READ_DATA_SOURCE_FUNCTIONALITY_MODULE_OPERATION_COUNT = LEAF_FUNCTIONALITY_MODULE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.core.model.functionality_module.impl.ReportCellCreationFunctionalityModuleImpl <em>Report Cell Creation Functionality Module</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.core.model.functionality_module.impl.ReportCellCreationFunctionalityModuleImpl
	 * @see org.eclipse.efbt.cocalimo.core.model.functionality_module.impl.Functionality_modulePackageImpl#getReportCellCreationFunctionalityModule()
	 * @generated
	 */
	int REPORT_CELL_CREATION_FUNCTIONALITY_MODULE = 7;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REPORT_CELL_CREATION_FUNCTIONALITY_MODULE__NAME = LEAF_FUNCTIONALITY_MODULE__NAME;

	/**
	 * The feature id for the '<em><b>Used Module</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REPORT_CELL_CREATION_FUNCTIONALITY_MODULE__USED_MODULE = LEAF_FUNCTIONALITY_MODULE__USED_MODULE;

	/**
	 * The feature id for the '<em><b>Scenario</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REPORT_CELL_CREATION_FUNCTIONALITY_MODULE__SCENARIO = LEAF_FUNCTIONALITY_MODULE__SCENARIO;

	/**
	 * The feature id for the '<em><b>Scenario Set</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REPORT_CELL_CREATION_FUNCTIONALITY_MODULE__SCENARIO_SET = LEAF_FUNCTIONALITY_MODULE__SCENARIO_SET;

	/**
	 * The feature id for the '<em><b>Used In Subset</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REPORT_CELL_CREATION_FUNCTIONALITY_MODULE__USED_IN_SUBSET = LEAF_FUNCTIONALITY_MODULE__USED_IN_SUBSET;

	/**
	 * The number of structural features of the '<em>Report Cell Creation Functionality Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REPORT_CELL_CREATION_FUNCTIONALITY_MODULE_FEATURE_COUNT = LEAF_FUNCTIONALITY_MODULE_FEATURE_COUNT + 0;

	/**
	 * The number of operations of the '<em>Report Cell Creation Functionality Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REPORT_CELL_CREATION_FUNCTIONALITY_MODULE_OPERATION_COUNT = LEAF_FUNCTIONALITY_MODULE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.core.model.functionality_module.impl.FunctionalityModuleModuleImpl <em>Functionality Module Module</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.core.model.functionality_module.impl.FunctionalityModuleModuleImpl
	 * @see org.eclipse.efbt.cocalimo.core.model.functionality_module.impl.Functionality_modulePackageImpl#getFunctionalityModuleModule()
	 * @generated
	 */
	int FUNCTIONALITY_MODULE_MODULE = 9;

	/**
	 * The feature id for the '<em><b>Dependencies</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FUNCTIONALITY_MODULE_MODULE__DEPENDENCIES = Module_managementPackage.MODULE__DEPENDENCIES;

	/**
	 * The feature id for the '<em><b>The Description</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FUNCTIONALITY_MODULE_MODULE__THE_DESCRIPTION = Module_managementPackage.MODULE__THE_DESCRIPTION;

	/**
	 * The feature id for the '<em><b>License</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FUNCTIONALITY_MODULE_MODULE__LICENSE = Module_managementPackage.MODULE__LICENSE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FUNCTIONALITY_MODULE_MODULE__NAME = Module_managementPackage.MODULE__NAME;

	/**
	 * The feature id for the '<em><b>Version</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FUNCTIONALITY_MODULE_MODULE__VERSION = Module_managementPackage.MODULE__VERSION;

	/**
	 * The feature id for the '<em><b>Long Name</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FUNCTIONALITY_MODULE_MODULE__LONG_NAME = Module_managementPackage.MODULE__LONG_NAME;

	/**
	 * The feature id for the '<em><b>Functionality Modules</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FUNCTIONALITY_MODULE_MODULE__FUNCTIONALITY_MODULES = Module_managementPackage.MODULE_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Functionality Module Tags</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FUNCTIONALITY_MODULE_MODULE__FUNCTIONALITY_MODULE_TAGS = Module_managementPackage.MODULE_FEATURE_COUNT + 1;

	/**
	 * The feature id for the '<em><b>Sceanrio Tags</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FUNCTIONALITY_MODULE_MODULE__SCEANRIO_TAGS = Module_managementPackage.MODULE_FEATURE_COUNT + 2;

	/**
	 * The number of structural features of the '<em>Functionality Module Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FUNCTIONALITY_MODULE_MODULE_FEATURE_COUNT = Module_managementPackage.MODULE_FEATURE_COUNT + 3;

	/**
	 * The number of operations of the '<em>Functionality Module Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FUNCTIONALITY_MODULE_MODULE_OPERATION_COUNT = Module_managementPackage.MODULE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.core.model.functionality_module.impl.FunctionalityModuleTagImpl <em>Functionality Module Tag</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.core.model.functionality_module.impl.FunctionalityModuleTagImpl
	 * @see org.eclipse.efbt.cocalimo.core.model.functionality_module.impl.Functionality_modulePackageImpl#getFunctionalityModuleTag()
	 * @generated
	 */
	int FUNCTIONALITY_MODULE_TAG = 10;

	/**
	 * The feature id for the '<em><b>Display Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FUNCTIONALITY_MODULE_TAG__DISPLAY_NAME = Requirements_textPackage.TAG__DISPLAY_NAME;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FUNCTIONALITY_MODULE_TAG__NAME = Requirements_textPackage.TAG__NAME;

	/**
	 * The feature id for the '<em><b>Requirements</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FUNCTIONALITY_MODULE_TAG__REQUIREMENTS = Requirements_textPackage.TAG__REQUIREMENTS;

	/**
	 * The feature id for the '<em><b>Functionality Module</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FUNCTIONALITY_MODULE_TAG__FUNCTIONALITY_MODULE = Requirements_textPackage.TAG_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Functionality Module Tag</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FUNCTIONALITY_MODULE_TAG_FEATURE_COUNT = Requirements_textPackage.TAG_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Functionality Module Tag</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FUNCTIONALITY_MODULE_TAG_OPERATION_COUNT = Requirements_textPackage.TAG_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.core.model.functionality_module.impl.ValidationFunctionalityModuleImpl <em>Validation Functionality Module</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.core.model.functionality_module.impl.ValidationFunctionalityModuleImpl
	 * @see org.eclipse.efbt.cocalimo.core.model.functionality_module.impl.Functionality_modulePackageImpl#getValidationFunctionalityModule()
	 * @generated
	 */
	int VALIDATION_FUNCTIONALITY_MODULE = 11;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VALIDATION_FUNCTIONALITY_MODULE__NAME = LEAF_FUNCTIONALITY_MODULE__NAME;

	/**
	 * The feature id for the '<em><b>Used Module</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VALIDATION_FUNCTIONALITY_MODULE__USED_MODULE = LEAF_FUNCTIONALITY_MODULE__USED_MODULE;

	/**
	 * The feature id for the '<em><b>Scenario</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VALIDATION_FUNCTIONALITY_MODULE__SCENARIO = LEAF_FUNCTIONALITY_MODULE__SCENARIO;

	/**
	 * The feature id for the '<em><b>Scenario Set</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VALIDATION_FUNCTIONALITY_MODULE__SCENARIO_SET = LEAF_FUNCTIONALITY_MODULE__SCENARIO_SET;

	/**
	 * The feature id for the '<em><b>Used In Subset</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VALIDATION_FUNCTIONALITY_MODULE__USED_IN_SUBSET = LEAF_FUNCTIONALITY_MODULE__USED_IN_SUBSET;

	/**
	 * The number of structural features of the '<em>Validation Functionality Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VALIDATION_FUNCTIONALITY_MODULE_FEATURE_COUNT = LEAF_FUNCTIONALITY_MODULE_FEATURE_COUNT + 0;

	/**
	 * The number of operations of the '<em>Validation Functionality Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VALIDATION_FUNCTIONALITY_MODULE_OPERATION_COUNT = LEAF_FUNCTIONALITY_MODULE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.core.model.functionality_module.impl.ParallelChoiceFunctionalityModuleImpl <em>Parallel Choice Functionality Module</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.core.model.functionality_module.impl.ParallelChoiceFunctionalityModuleImpl
	 * @see org.eclipse.efbt.cocalimo.core.model.functionality_module.impl.Functionality_modulePackageImpl#getParallelChoiceFunctionalityModule()
	 * @generated
	 */
	int PARALLEL_CHOICE_FUNCTIONALITY_MODULE = 12;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PARALLEL_CHOICE_FUNCTIONALITY_MODULE__NAME = FUNCTIONALITY_MODULE_SET__NAME;

	/**
	 * The feature id for the '<em><b>Used Module</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PARALLEL_CHOICE_FUNCTIONALITY_MODULE__USED_MODULE = FUNCTIONALITY_MODULE_SET__USED_MODULE;

	/**
	 * The feature id for the '<em><b>Scenario</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PARALLEL_CHOICE_FUNCTIONALITY_MODULE__SCENARIO = FUNCTIONALITY_MODULE_SET__SCENARIO;

	/**
	 * The feature id for the '<em><b>Scenario Set</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PARALLEL_CHOICE_FUNCTIONALITY_MODULE__SCENARIO_SET = FUNCTIONALITY_MODULE_SET__SCENARIO_SET;

	/**
	 * The feature id for the '<em><b>Used In Subset</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PARALLEL_CHOICE_FUNCTIONALITY_MODULE__USED_IN_SUBSET = FUNCTIONALITY_MODULE_SET__USED_IN_SUBSET;

	/**
	 * The feature id for the '<em><b>Sub Modules</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PARALLEL_CHOICE_FUNCTIONALITY_MODULE__SUB_MODULES = FUNCTIONALITY_MODULE_SET__SUB_MODULES;

	/**
	 * The number of structural features of the '<em>Parallel Choice Functionality Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PARALLEL_CHOICE_FUNCTIONALITY_MODULE_FEATURE_COUNT = FUNCTIONALITY_MODULE_SET_FEATURE_COUNT + 0;

	/**
	 * The number of operations of the '<em>Parallel Choice Functionality Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PARALLEL_CHOICE_FUNCTIONALITY_MODULE_OPERATION_COUNT = FUNCTIONALITY_MODULE_SET_OPERATION_COUNT + 0;


	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.core.model.functionality_module.DataProcessingFunctionalityModule <em>Data Processing Functionality Module</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Data Processing Functionality Module</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.functionality_module.DataProcessingFunctionalityModule
	 * @generated
	 */
	EClass getDataProcessingFunctionalityModule();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.core.model.functionality_module.DataProcessingFunctionalityModule#getEnrichedAttribute <em>Enriched Attribute</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Enriched Attribute</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.functionality_module.DataProcessingFunctionalityModule#getEnrichedAttribute()
	 * @see #getDataProcessingFunctionalityModule()
	 * @generated
	 */
	EReference getDataProcessingFunctionalityModule_EnrichedAttribute();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.core.model.functionality_module.LeafFunctionalityModule <em>Leaf Functionality Module</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Leaf Functionality Module</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.functionality_module.LeafFunctionalityModule
	 * @generated
	 */
	EClass getLeafFunctionalityModule();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.core.model.functionality_module.ParallelFunctionalityModule <em>Parallel Functionality Module</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Parallel Functionality Module</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.functionality_module.ParallelFunctionalityModule
	 * @generated
	 */
	EClass getParallelFunctionalityModule();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.core.model.functionality_module.ScenarioSetFunctionalityModule <em>Scenario Set Functionality Module</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Scenario Set Functionality Module</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.functionality_module.ScenarioSetFunctionalityModule
	 * @generated
	 */
	EClass getScenarioSetFunctionalityModule();

	/**
	 * Returns the meta object for the containment reference '{@link org.eclipse.efbt.cocalimo.core.model.functionality_module.ScenarioSetFunctionalityModule#getScenarioSetChoice <em>Scenario Set Choice</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Scenario Set Choice</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.functionality_module.ScenarioSetFunctionalityModule#getScenarioSetChoice()
	 * @see #getScenarioSetFunctionalityModule()
	 * @generated
	 */
	EReference getScenarioSetFunctionalityModule_ScenarioSetChoice();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.core.model.functionality_module.SerialFunctionalityModule <em>Serial Functionality Module</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Serial Functionality Module</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.functionality_module.SerialFunctionalityModule
	 * @generated
	 */
	EClass getSerialFunctionalityModule();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.core.model.functionality_module.FunctionalityModuleSet <em>Functionality Module Set</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Functionality Module Set</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.functionality_module.FunctionalityModuleSet
	 * @generated
	 */
	EClass getFunctionalityModuleSet();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.cocalimo.core.model.functionality_module.FunctionalityModuleSet#getSubModules <em>Sub Modules</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Sub Modules</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.functionality_module.FunctionalityModuleSet#getSubModules()
	 * @see #getFunctionalityModuleSet()
	 * @generated
	 */
	EReference getFunctionalityModuleSet_SubModules();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.core.model.functionality_module.ReadDataSourceFunctionalityModule <em>Read Data Source Functionality Module</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Read Data Source Functionality Module</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.functionality_module.ReadDataSourceFunctionalityModule
	 * @generated
	 */
	EClass getReadDataSourceFunctionalityModule();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.core.model.functionality_module.ReadDataSourceFunctionalityModule#getEntity <em>Entity</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Entity</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.functionality_module.ReadDataSourceFunctionalityModule#getEntity()
	 * @see #getReadDataSourceFunctionalityModule()
	 * @generated
	 */
	EReference getReadDataSourceFunctionalityModule_Entity();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.core.model.functionality_module.ReportCellCreationFunctionalityModule <em>Report Cell Creation Functionality Module</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Report Cell Creation Functionality Module</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.functionality_module.ReportCellCreationFunctionalityModule
	 * @generated
	 */
	EClass getReportCellCreationFunctionalityModule();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.core.model.functionality_module.FunctionalityModule <em>Functionality Module</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Functionality Module</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.functionality_module.FunctionalityModule
	 * @generated
	 */
	EClass getFunctionalityModule();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.core.model.functionality_module.FunctionalityModule#getName <em>Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Name</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.functionality_module.FunctionalityModule#getName()
	 * @see #getFunctionalityModule()
	 * @generated
	 */
	EAttribute getFunctionalityModule_Name();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.core.model.functionality_module.FunctionalityModule#getUsedModule <em>Used Module</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Used Module</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.functionality_module.FunctionalityModule#getUsedModule()
	 * @see #getFunctionalityModule()
	 * @generated
	 */
	EReference getFunctionalityModule_UsedModule();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.core.model.functionality_module.FunctionalityModule#getScenario <em>Scenario</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Scenario</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.functionality_module.FunctionalityModule#getScenario()
	 * @see #getFunctionalityModule()
	 * @generated
	 */
	EReference getFunctionalityModule_Scenario();

	/**
	 * Returns the meta object for the containment reference '{@link org.eclipse.efbt.cocalimo.core.model.functionality_module.FunctionalityModule#getScenarioSet <em>Scenario Set</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Scenario Set</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.functionality_module.FunctionalityModule#getScenarioSet()
	 * @see #getFunctionalityModule()
	 * @generated
	 */
	EReference getFunctionalityModule_ScenarioSet();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.core.model.functionality_module.FunctionalityModule#isUsedInSubset <em>Used In Subset</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Used In Subset</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.functionality_module.FunctionalityModule#isUsedInSubset()
	 * @see #getFunctionalityModule()
	 * @generated
	 */
	EAttribute getFunctionalityModule_UsedInSubset();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.core.model.functionality_module.FunctionalityModuleModule <em>Functionality Module Module</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Functionality Module Module</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.functionality_module.FunctionalityModuleModule
	 * @generated
	 */
	EClass getFunctionalityModuleModule();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.cocalimo.core.model.functionality_module.FunctionalityModuleModule#getFunctionalityModules <em>Functionality Modules</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Functionality Modules</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.functionality_module.FunctionalityModuleModule#getFunctionalityModules()
	 * @see #getFunctionalityModuleModule()
	 * @generated
	 */
	EReference getFunctionalityModuleModule_FunctionalityModules();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.cocalimo.core.model.functionality_module.FunctionalityModuleModule#getFunctionalityModuleTags <em>Functionality Module Tags</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Functionality Module Tags</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.functionality_module.FunctionalityModuleModule#getFunctionalityModuleTags()
	 * @see #getFunctionalityModuleModule()
	 * @generated
	 */
	EReference getFunctionalityModuleModule_FunctionalityModuleTags();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.cocalimo.core.model.functionality_module.FunctionalityModuleModule#getSceanrioTags <em>Sceanrio Tags</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Sceanrio Tags</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.functionality_module.FunctionalityModuleModule#getSceanrioTags()
	 * @see #getFunctionalityModuleModule()
	 * @generated
	 */
	EReference getFunctionalityModuleModule_SceanrioTags();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.core.model.functionality_module.FunctionalityModuleTag <em>Functionality Module Tag</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Functionality Module Tag</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.functionality_module.FunctionalityModuleTag
	 * @generated
	 */
	EClass getFunctionalityModuleTag();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.core.model.functionality_module.FunctionalityModuleTag#getFunctionalityModule <em>Functionality Module</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Functionality Module</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.functionality_module.FunctionalityModuleTag#getFunctionalityModule()
	 * @see #getFunctionalityModuleTag()
	 * @generated
	 */
	EReference getFunctionalityModuleTag_FunctionalityModule();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.core.model.functionality_module.ValidationFunctionalityModule <em>Validation Functionality Module</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Validation Functionality Module</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.functionality_module.ValidationFunctionalityModule
	 * @generated
	 */
	EClass getValidationFunctionalityModule();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.core.model.functionality_module.ParallelChoiceFunctionalityModule <em>Parallel Choice Functionality Module</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Parallel Choice Functionality Module</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.functionality_module.ParallelChoiceFunctionalityModule
	 * @generated
	 */
	EClass getParallelChoiceFunctionalityModule();

	/**
	 * Returns the factory that creates the instances of the model.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the factory that creates the instances of the model.
	 * @generated
	 */
	Functionality_moduleFactory getFunctionality_moduleFactory();

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
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.core.model.functionality_module.impl.DataProcessingFunctionalityModuleImpl <em>Data Processing Functionality Module</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.core.model.functionality_module.impl.DataProcessingFunctionalityModuleImpl
		 * @see org.eclipse.efbt.cocalimo.core.model.functionality_module.impl.Functionality_modulePackageImpl#getDataProcessingFunctionalityModule()
		 * @generated
		 */
		EClass DATA_PROCESSING_FUNCTIONALITY_MODULE = eINSTANCE.getDataProcessingFunctionalityModule();

		/**
		 * The meta object literal for the '<em><b>Enriched Attribute</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference DATA_PROCESSING_FUNCTIONALITY_MODULE__ENRICHED_ATTRIBUTE = eINSTANCE.getDataProcessingFunctionalityModule_EnrichedAttribute();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.core.model.functionality_module.impl.LeafFunctionalityModuleImpl <em>Leaf Functionality Module</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.core.model.functionality_module.impl.LeafFunctionalityModuleImpl
		 * @see org.eclipse.efbt.cocalimo.core.model.functionality_module.impl.Functionality_modulePackageImpl#getLeafFunctionalityModule()
		 * @generated
		 */
		EClass LEAF_FUNCTIONALITY_MODULE = eINSTANCE.getLeafFunctionalityModule();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.core.model.functionality_module.impl.ParallelFunctionalityModuleImpl <em>Parallel Functionality Module</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.core.model.functionality_module.impl.ParallelFunctionalityModuleImpl
		 * @see org.eclipse.efbt.cocalimo.core.model.functionality_module.impl.Functionality_modulePackageImpl#getParallelFunctionalityModule()
		 * @generated
		 */
		EClass PARALLEL_FUNCTIONALITY_MODULE = eINSTANCE.getParallelFunctionalityModule();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.core.model.functionality_module.impl.ScenarioSetFunctionalityModuleImpl <em>Scenario Set Functionality Module</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.core.model.functionality_module.impl.ScenarioSetFunctionalityModuleImpl
		 * @see org.eclipse.efbt.cocalimo.core.model.functionality_module.impl.Functionality_modulePackageImpl#getScenarioSetFunctionalityModule()
		 * @generated
		 */
		EClass SCENARIO_SET_FUNCTIONALITY_MODULE = eINSTANCE.getScenarioSetFunctionalityModule();

		/**
		 * The meta object literal for the '<em><b>Scenario Set Choice</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference SCENARIO_SET_FUNCTIONALITY_MODULE__SCENARIO_SET_CHOICE = eINSTANCE.getScenarioSetFunctionalityModule_ScenarioSetChoice();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.core.model.functionality_module.impl.SerialFunctionalityModuleImpl <em>Serial Functionality Module</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.core.model.functionality_module.impl.SerialFunctionalityModuleImpl
		 * @see org.eclipse.efbt.cocalimo.core.model.functionality_module.impl.Functionality_modulePackageImpl#getSerialFunctionalityModule()
		 * @generated
		 */
		EClass SERIAL_FUNCTIONALITY_MODULE = eINSTANCE.getSerialFunctionalityModule();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.core.model.functionality_module.impl.FunctionalityModuleSetImpl <em>Functionality Module Set</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.core.model.functionality_module.impl.FunctionalityModuleSetImpl
		 * @see org.eclipse.efbt.cocalimo.core.model.functionality_module.impl.Functionality_modulePackageImpl#getFunctionalityModuleSet()
		 * @generated
		 */
		EClass FUNCTIONALITY_MODULE_SET = eINSTANCE.getFunctionalityModuleSet();

		/**
		 * The meta object literal for the '<em><b>Sub Modules</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference FUNCTIONALITY_MODULE_SET__SUB_MODULES = eINSTANCE.getFunctionalityModuleSet_SubModules();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.core.model.functionality_module.impl.ReadDataSourceFunctionalityModuleImpl <em>Read Data Source Functionality Module</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.core.model.functionality_module.impl.ReadDataSourceFunctionalityModuleImpl
		 * @see org.eclipse.efbt.cocalimo.core.model.functionality_module.impl.Functionality_modulePackageImpl#getReadDataSourceFunctionalityModule()
		 * @generated
		 */
		EClass READ_DATA_SOURCE_FUNCTIONALITY_MODULE = eINSTANCE.getReadDataSourceFunctionalityModule();

		/**
		 * The meta object literal for the '<em><b>Entity</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference READ_DATA_SOURCE_FUNCTIONALITY_MODULE__ENTITY = eINSTANCE.getReadDataSourceFunctionalityModule_Entity();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.core.model.functionality_module.impl.ReportCellCreationFunctionalityModuleImpl <em>Report Cell Creation Functionality Module</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.core.model.functionality_module.impl.ReportCellCreationFunctionalityModuleImpl
		 * @see org.eclipse.efbt.cocalimo.core.model.functionality_module.impl.Functionality_modulePackageImpl#getReportCellCreationFunctionalityModule()
		 * @generated
		 */
		EClass REPORT_CELL_CREATION_FUNCTIONALITY_MODULE = eINSTANCE.getReportCellCreationFunctionalityModule();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.core.model.functionality_module.impl.FunctionalityModuleImpl <em>Functionality Module</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.core.model.functionality_module.impl.FunctionalityModuleImpl
		 * @see org.eclipse.efbt.cocalimo.core.model.functionality_module.impl.Functionality_modulePackageImpl#getFunctionalityModule()
		 * @generated
		 */
		EClass FUNCTIONALITY_MODULE = eINSTANCE.getFunctionalityModule();

		/**
		 * The meta object literal for the '<em><b>Name</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute FUNCTIONALITY_MODULE__NAME = eINSTANCE.getFunctionalityModule_Name();

		/**
		 * The meta object literal for the '<em><b>Used Module</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference FUNCTIONALITY_MODULE__USED_MODULE = eINSTANCE.getFunctionalityModule_UsedModule();

		/**
		 * The meta object literal for the '<em><b>Scenario</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference FUNCTIONALITY_MODULE__SCENARIO = eINSTANCE.getFunctionalityModule_Scenario();

		/**
		 * The meta object literal for the '<em><b>Scenario Set</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference FUNCTIONALITY_MODULE__SCENARIO_SET = eINSTANCE.getFunctionalityModule_ScenarioSet();

		/**
		 * The meta object literal for the '<em><b>Used In Subset</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute FUNCTIONALITY_MODULE__USED_IN_SUBSET = eINSTANCE.getFunctionalityModule_UsedInSubset();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.core.model.functionality_module.impl.FunctionalityModuleModuleImpl <em>Functionality Module Module</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.core.model.functionality_module.impl.FunctionalityModuleModuleImpl
		 * @see org.eclipse.efbt.cocalimo.core.model.functionality_module.impl.Functionality_modulePackageImpl#getFunctionalityModuleModule()
		 * @generated
		 */
		EClass FUNCTIONALITY_MODULE_MODULE = eINSTANCE.getFunctionalityModuleModule();

		/**
		 * The meta object literal for the '<em><b>Functionality Modules</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference FUNCTIONALITY_MODULE_MODULE__FUNCTIONALITY_MODULES = eINSTANCE.getFunctionalityModuleModule_FunctionalityModules();

		/**
		 * The meta object literal for the '<em><b>Functionality Module Tags</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference FUNCTIONALITY_MODULE_MODULE__FUNCTIONALITY_MODULE_TAGS = eINSTANCE.getFunctionalityModuleModule_FunctionalityModuleTags();

		/**
		 * The meta object literal for the '<em><b>Sceanrio Tags</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference FUNCTIONALITY_MODULE_MODULE__SCEANRIO_TAGS = eINSTANCE.getFunctionalityModuleModule_SceanrioTags();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.core.model.functionality_module.impl.FunctionalityModuleTagImpl <em>Functionality Module Tag</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.core.model.functionality_module.impl.FunctionalityModuleTagImpl
		 * @see org.eclipse.efbt.cocalimo.core.model.functionality_module.impl.Functionality_modulePackageImpl#getFunctionalityModuleTag()
		 * @generated
		 */
		EClass FUNCTIONALITY_MODULE_TAG = eINSTANCE.getFunctionalityModuleTag();

		/**
		 * The meta object literal for the '<em><b>Functionality Module</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference FUNCTIONALITY_MODULE_TAG__FUNCTIONALITY_MODULE = eINSTANCE.getFunctionalityModuleTag_FunctionalityModule();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.core.model.functionality_module.impl.ValidationFunctionalityModuleImpl <em>Validation Functionality Module</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.core.model.functionality_module.impl.ValidationFunctionalityModuleImpl
		 * @see org.eclipse.efbt.cocalimo.core.model.functionality_module.impl.Functionality_modulePackageImpl#getValidationFunctionalityModule()
		 * @generated
		 */
		EClass VALIDATION_FUNCTIONALITY_MODULE = eINSTANCE.getValidationFunctionalityModule();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.core.model.functionality_module.impl.ParallelChoiceFunctionalityModuleImpl <em>Parallel Choice Functionality Module</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.core.model.functionality_module.impl.ParallelChoiceFunctionalityModuleImpl
		 * @see org.eclipse.efbt.cocalimo.core.model.functionality_module.impl.Functionality_modulePackageImpl#getParallelChoiceFunctionalityModule()
		 * @generated
		 */
		EClass PARALLEL_CHOICE_FUNCTIONALITY_MODULE = eINSTANCE.getParallelChoiceFunctionalityModule();

	}

} //Functionality_modulePackage
