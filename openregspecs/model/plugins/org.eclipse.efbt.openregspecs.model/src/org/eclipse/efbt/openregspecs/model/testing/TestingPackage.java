/**
 */
package org.eclipse.efbt.openregspecs.model.testing;

import org.eclipse.efbt.openregspecs.model.module_management.Module_managementPackage;

import org.eclipse.efbt.openregspecs.model.requirements_text.Requirements_textPackage;

import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;
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
 * @see org.eclipse.efbt.openregspecs.model.testing.TestingFactory
 * @model kind="package"
 *        annotation="license license='Copyright (c) 2020 Bird Software Solutions Ltd\n All rights reserved. This file and the accompanying materials are made available under the terms of the Eclipse Public License v2.0 which accompanies this distribution, and is available at http://www.eclipse.org/legal/epl-v20.html\n\nSPDX-License-Identifier: EPL-2.0 \n\n Contributors:\n Neil Mackenzie - initial API and implementation\r'"
 * @generated
 */
public interface TestingPackage extends EPackage {
	/**
	 * The package name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNAME = "testing";

	/**
	 * The package namespace URI.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNS_URI = "http://www.eclipse.org/efbt/testing";

	/**
	 * The package namespace name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNS_PREFIX = "testing";

	/**
	 * The singleton instance of the package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	TestingPackage eINSTANCE = org.eclipse.efbt.openregspecs.model.testing.impl.TestingPackageImpl.init();

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.openregspecs.model.testing.impl.LogicalTransformationModuleImpl <em>Logical Transformation Module</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.openregspecs.model.testing.impl.LogicalTransformationModuleImpl
	 * @see org.eclipse.efbt.openregspecs.model.testing.impl.TestingPackageImpl#getLogicalTransformationModule()
	 * @generated
	 */
	int LOGICAL_TRANSFORMATION_MODULE = 0;

	/**
	 * The feature id for the '<em><b>Dependencies</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int LOGICAL_TRANSFORMATION_MODULE__DEPENDENCIES = Module_managementPackage.MODULE__DEPENDENCIES;

	/**
	 * The feature id for the '<em><b>The Description</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int LOGICAL_TRANSFORMATION_MODULE__THE_DESCRIPTION = Module_managementPackage.MODULE__THE_DESCRIPTION;

	/**
	 * The feature id for the '<em><b>License</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int LOGICAL_TRANSFORMATION_MODULE__LICENSE = Module_managementPackage.MODULE__LICENSE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int LOGICAL_TRANSFORMATION_MODULE__NAME = Module_managementPackage.MODULE__NAME;

	/**
	 * The feature id for the '<em><b>Version</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int LOGICAL_TRANSFORMATION_MODULE__VERSION = Module_managementPackage.MODULE__VERSION;

	/**
	 * The feature id for the '<em><b>Long Name</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int LOGICAL_TRANSFORMATION_MODULE__LONG_NAME = Module_managementPackage.MODULE__LONG_NAME;

	/**
	 * The feature id for the '<em><b>Task Tags</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int LOGICAL_TRANSFORMATION_MODULE__TASK_TAGS = Module_managementPackage.MODULE_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Scenario Tags</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int LOGICAL_TRANSFORMATION_MODULE__SCENARIO_TAGS = Module_managementPackage.MODULE_FEATURE_COUNT + 1;

	/**
	 * The feature id for the '<em><b>Sub Process</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int LOGICAL_TRANSFORMATION_MODULE__SUB_PROCESS = Module_managementPackage.MODULE_FEATURE_COUNT + 2;

	/**
	 * The feature id for the '<em><b>Requirements</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int LOGICAL_TRANSFORMATION_MODULE__REQUIREMENTS = Module_managementPackage.MODULE_FEATURE_COUNT + 3;

	/**
	 * The feature id for the '<em><b>Test Modules</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int LOGICAL_TRANSFORMATION_MODULE__TEST_MODULES = Module_managementPackage.MODULE_FEATURE_COUNT + 4;

	/**
	 * The feature id for the '<em><b>Selection Layers</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int LOGICAL_TRANSFORMATION_MODULE__SELECTION_LAYERS = Module_managementPackage.MODULE_FEATURE_COUNT + 5;

	/**
	 * The number of structural features of the '<em>Logical Transformation Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int LOGICAL_TRANSFORMATION_MODULE_FEATURE_COUNT = Module_managementPackage.MODULE_FEATURE_COUNT + 6;

	/**
	 * The number of operations of the '<em>Logical Transformation Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int LOGICAL_TRANSFORMATION_MODULE_OPERATION_COUNT = Module_managementPackage.MODULE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.openregspecs.model.testing.impl.ActivityTagImpl <em>Activity Tag</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.openregspecs.model.testing.impl.ActivityTagImpl
	 * @see org.eclipse.efbt.openregspecs.model.testing.impl.TestingPackageImpl#getActivityTag()
	 * @generated
	 */
	int ACTIVITY_TAG = 1;

	/**
	 * The feature id for the '<em><b>Display Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ACTIVITY_TAG__DISPLAY_NAME = Requirements_textPackage.TAG__DISPLAY_NAME;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ACTIVITY_TAG__NAME = Requirements_textPackage.TAG__NAME;

	/**
	 * The feature id for the '<em><b>Requirements</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ACTIVITY_TAG__REQUIREMENTS = Requirements_textPackage.TAG__REQUIREMENTS;

	/**
	 * The feature id for the '<em><b>Activity</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ACTIVITY_TAG__ACTIVITY = Requirements_textPackage.TAG_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Activity Tag</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ACTIVITY_TAG_FEATURE_COUNT = Requirements_textPackage.TAG_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Activity Tag</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ACTIVITY_TAG_OPERATION_COUNT = Requirements_textPackage.TAG_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.openregspecs.model.testing.impl.ScenarioImpl <em>Scenario</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.openregspecs.model.testing.impl.ScenarioImpl
	 * @see org.eclipse.efbt.openregspecs.model.testing.impl.TestingPackageImpl#getScenario()
	 * @generated
	 */
	int SCENARIO = 2;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SCENARIO__NAME = 0;

	/**
	 * The feature id for the '<em><b>Invisible</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SCENARIO__INVISIBLE = 1;

	/**
	 * The feature id for the '<em><b>Description</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SCENARIO__DESCRIPTION = 2;

	/**
	 * The feature id for the '<em><b>Required Attributes</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SCENARIO__REQUIRED_ATTRIBUTES = 3;

	/**
	 * The feature id for the '<em><b>Data constraints</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SCENARIO__DATA_CONSTRAINTS = 4;

	/**
	 * The number of structural features of the '<em>Scenario</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SCENARIO_FEATURE_COUNT = 5;

	/**
	 * The number of operations of the '<em>Scenario</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SCENARIO_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.openregspecs.model.testing.impl.ScenarioTagImpl <em>Scenario Tag</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.openregspecs.model.testing.impl.ScenarioTagImpl
	 * @see org.eclipse.efbt.openregspecs.model.testing.impl.TestingPackageImpl#getScenarioTag()
	 * @generated
	 */
	int SCENARIO_TAG = 3;

	/**
	 * The feature id for the '<em><b>Display Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SCENARIO_TAG__DISPLAY_NAME = Requirements_textPackage.TAG__DISPLAY_NAME;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SCENARIO_TAG__NAME = Requirements_textPackage.TAG__NAME;

	/**
	 * The feature id for the '<em><b>Requirements</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SCENARIO_TAG__REQUIREMENTS = Requirements_textPackage.TAG__REQUIREMENTS;

	/**
	 * The feature id for the '<em><b>Scenario</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SCENARIO_TAG__SCENARIO = Requirements_textPackage.TAG_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Scenario Tag</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SCENARIO_TAG_FEATURE_COUNT = Requirements_textPackage.TAG_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Scenario Tag</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SCENARIO_TAG_OPERATION_COUNT = Requirements_textPackage.TAG_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.openregspecs.model.testing.impl.TestImpl <em>Test</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.openregspecs.model.testing.impl.TestImpl
	 * @see org.eclipse.efbt.openregspecs.model.testing.impl.TestingPackageImpl#getTest()
	 * @generated
	 */
	int TEST = 4;

	/**
	 * The feature id for the '<em><b>Input Data</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TEST__INPUT_DATA = 0;

	/**
	 * The feature id for the '<em><b>Expected Result</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TEST__EXPECTED_RESULT = 1;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TEST__NAME = 2;

	/**
	 * The feature id for the '<em><b>Scope</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TEST__SCOPE = 3;

	/**
	 * The number of structural features of the '<em>Test</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TEST_FEATURE_COUNT = 4;

	/**
	 * The number of operations of the '<em>Test</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TEST_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.openregspecs.model.testing.impl.TestModuleImpl <em>Test Module</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.openregspecs.model.testing.impl.TestModuleImpl
	 * @see org.eclipse.efbt.openregspecs.model.testing.impl.TestingPackageImpl#getTestModule()
	 * @generated
	 */
	int TEST_MODULE = 5;

	/**
	 * The feature id for the '<em><b>Dependencies</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TEST_MODULE__DEPENDENCIES = Module_managementPackage.MODULE__DEPENDENCIES;

	/**
	 * The feature id for the '<em><b>The Description</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TEST_MODULE__THE_DESCRIPTION = Module_managementPackage.MODULE__THE_DESCRIPTION;

	/**
	 * The feature id for the '<em><b>License</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TEST_MODULE__LICENSE = Module_managementPackage.MODULE__LICENSE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TEST_MODULE__NAME = Module_managementPackage.MODULE__NAME;

	/**
	 * The feature id for the '<em><b>Version</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TEST_MODULE__VERSION = Module_managementPackage.MODULE__VERSION;

	/**
	 * The feature id for the '<em><b>Long Name</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TEST_MODULE__LONG_NAME = Module_managementPackage.MODULE__LONG_NAME;

	/**
	 * The feature id for the '<em><b>Tests</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TEST_MODULE__TESTS = Module_managementPackage.MODULE_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Test Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TEST_MODULE_FEATURE_COUNT = Module_managementPackage.MODULE_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Test Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TEST_MODULE_OPERATION_COUNT = Module_managementPackage.MODULE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.openregspecs.model.testing.impl.DataConstraintImpl <em>Data Constraint</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.openregspecs.model.testing.impl.DataConstraintImpl
	 * @see org.eclipse.efbt.openregspecs.model.testing.impl.TestingPackageImpl#getDataConstraint()
	 * @generated
	 */
	int DATA_CONSTRAINT = 6;

	/**
	 * The feature id for the '<em><b>Attr1</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DATA_CONSTRAINT__ATTR1 = 0;

	/**
	 * The feature id for the '<em><b>Attr2</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DATA_CONSTRAINT__ATTR2 = 1;

	/**
	 * The feature id for the '<em><b>Comparison</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DATA_CONSTRAINT__COMPARISON = 2;

	/**
	 * The number of structural features of the '<em>Data Constraint</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DATA_CONSTRAINT_FEATURE_COUNT = 3;

	/**
	 * The number of operations of the '<em>Data Constraint</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DATA_CONSTRAINT_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.openregspecs.model.testing.impl.SelectionLayerImpl <em>Selection Layer</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.openregspecs.model.testing.impl.SelectionLayerImpl
	 * @see org.eclipse.efbt.openregspecs.model.testing.impl.TestingPackageImpl#getSelectionLayer()
	 * @generated
	 */
	int SELECTION_LAYER = 7;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SELECTION_LAYER__NAME = 0;

	/**
	 * The feature id for the '<em><b>Invisible</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SELECTION_LAYER__INVISIBLE = 1;

	/**
	 * The feature id for the '<em><b>Generated Entity</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SELECTION_LAYER__GENERATED_ENTITY = 2;

	/**
	 * The number of structural features of the '<em>Selection Layer</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SELECTION_LAYER_FEATURE_COUNT = 3;

	/**
	 * The number of operations of the '<em>Selection Layer</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SELECTION_LAYER_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.openregspecs.model.testing.impl.TestScopeImpl <em>Test Scope</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.openregspecs.model.testing.impl.TestScopeImpl
	 * @see org.eclipse.efbt.openregspecs.model.testing.impl.TestingPackageImpl#getTestScope()
	 * @generated
	 */
	int TEST_SCOPE = 8;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TEST_SCOPE__NAME = 0;

	/**
	 * The number of structural features of the '<em>Test Scope</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TEST_SCOPE_FEATURE_COUNT = 1;

	/**
	 * The number of operations of the '<em>Test Scope</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TEST_SCOPE_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.openregspecs.model.testing.impl.UnitTestScopeImpl <em>Unit Test Scope</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.openregspecs.model.testing.impl.UnitTestScopeImpl
	 * @see org.eclipse.efbt.openregspecs.model.testing.impl.TestingPackageImpl#getUnitTestScope()
	 * @generated
	 */
	int UNIT_TEST_SCOPE = 9;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNIT_TEST_SCOPE__NAME = TEST_SCOPE__NAME;

	/**
	 * The feature id for the '<em><b>Scenarios</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNIT_TEST_SCOPE__SCENARIOS = TEST_SCOPE_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Unit Test Scope</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNIT_TEST_SCOPE_FEATURE_COUNT = TEST_SCOPE_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Unit Test Scope</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNIT_TEST_SCOPE_OPERATION_COUNT = TEST_SCOPE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.openregspecs.model.testing.impl.E2ETestScopeImpl <em>E2E Test Scope</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.openregspecs.model.testing.impl.E2ETestScopeImpl
	 * @see org.eclipse.efbt.openregspecs.model.testing.impl.TestingPackageImpl#getE2ETestScope()
	 * @generated
	 */
	int E2E_TEST_SCOPE = 10;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int E2E_TEST_SCOPE__NAME = TEST_SCOPE__NAME;

	/**
	 * The feature id for the '<em><b>Scenarios</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int E2E_TEST_SCOPE__SCENARIOS = TEST_SCOPE_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Layer</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int E2E_TEST_SCOPE__LAYER = TEST_SCOPE_FEATURE_COUNT + 1;

	/**
	 * The feature id for the '<em><b>Script Task</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int E2E_TEST_SCOPE__SCRIPT_TASK = TEST_SCOPE_FEATURE_COUNT + 2;

	/**
	 * The number of structural features of the '<em>E2E Test Scope</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int E2E_TEST_SCOPE_FEATURE_COUNT = TEST_SCOPE_FEATURE_COUNT + 3;

	/**
	 * The number of operations of the '<em>E2E Test Scope</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int E2E_TEST_SCOPE_OPERATION_COUNT = TEST_SCOPE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.openregspecs.model.testing.impl.CSVFileImpl <em>CSV File</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.openregspecs.model.testing.impl.CSVFileImpl
	 * @see org.eclipse.efbt.openregspecs.model.testing.impl.TestingPackageImpl#getCSVFile()
	 * @generated
	 */
	int CSV_FILE = 11;

	/**
	 * The feature id for the '<em><b>File Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CSV_FILE__FILE_NAME = 0;

	/**
	 * The number of structural features of the '<em>CSV File</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CSV_FILE_FEATURE_COUNT = 1;

	/**
	 * The number of operations of the '<em>CSV File</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CSV_FILE_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.openregspecs.model.testing.AttrComparison <em>Attr Comparison</em>}' enum.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.openregspecs.model.testing.AttrComparison
	 * @see org.eclipse.efbt.openregspecs.model.testing.impl.TestingPackageImpl#getAttrComparison()
	 * @generated
	 */
	int ATTR_COMPARISON = 12;


	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.openregspecs.model.testing.LogicalTransformationModule <em>Logical Transformation Module</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Logical Transformation Module</em>'.
	 * @see org.eclipse.efbt.openregspecs.model.testing.LogicalTransformationModule
	 * @generated
	 */
	EClass getLogicalTransformationModule();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.openregspecs.model.testing.LogicalTransformationModule#getTaskTags <em>Task Tags</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Task Tags</em>'.
	 * @see org.eclipse.efbt.openregspecs.model.testing.LogicalTransformationModule#getTaskTags()
	 * @see #getLogicalTransformationModule()
	 * @generated
	 */
	EReference getLogicalTransformationModule_TaskTags();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.openregspecs.model.testing.LogicalTransformationModule#getScenarioTags <em>Scenario Tags</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Scenario Tags</em>'.
	 * @see org.eclipse.efbt.openregspecs.model.testing.LogicalTransformationModule#getScenarioTags()
	 * @see #getLogicalTransformationModule()
	 * @generated
	 */
	EReference getLogicalTransformationModule_ScenarioTags();

	/**
	 * Returns the meta object for the containment reference '{@link org.eclipse.efbt.openregspecs.model.testing.LogicalTransformationModule#getSubProcess <em>Sub Process</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Sub Process</em>'.
	 * @see org.eclipse.efbt.openregspecs.model.testing.LogicalTransformationModule#getSubProcess()
	 * @see #getLogicalTransformationModule()
	 * @generated
	 */
	EReference getLogicalTransformationModule_SubProcess();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.openregspecs.model.testing.LogicalTransformationModule#getRequirements <em>Requirements</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Requirements</em>'.
	 * @see org.eclipse.efbt.openregspecs.model.testing.LogicalTransformationModule#getRequirements()
	 * @see #getLogicalTransformationModule()
	 * @generated
	 */
	EReference getLogicalTransformationModule_Requirements();

	/**
	 * Returns the meta object for the reference list '{@link org.eclipse.efbt.openregspecs.model.testing.LogicalTransformationModule#getTestModules <em>Test Modules</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference list '<em>Test Modules</em>'.
	 * @see org.eclipse.efbt.openregspecs.model.testing.LogicalTransformationModule#getTestModules()
	 * @see #getLogicalTransformationModule()
	 * @generated
	 */
	EReference getLogicalTransformationModule_TestModules();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.openregspecs.model.testing.LogicalTransformationModule#getSelectionLayers <em>Selection Layers</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Selection Layers</em>'.
	 * @see org.eclipse.efbt.openregspecs.model.testing.LogicalTransformationModule#getSelectionLayers()
	 * @see #getLogicalTransformationModule()
	 * @generated
	 */
	EReference getLogicalTransformationModule_SelectionLayers();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.openregspecs.model.testing.ActivityTag <em>Activity Tag</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Activity Tag</em>'.
	 * @see org.eclipse.efbt.openregspecs.model.testing.ActivityTag
	 * @generated
	 */
	EClass getActivityTag();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.openregspecs.model.testing.ActivityTag#getActivity <em>Activity</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Activity</em>'.
	 * @see org.eclipse.efbt.openregspecs.model.testing.ActivityTag#getActivity()
	 * @see #getActivityTag()
	 * @generated
	 */
	EReference getActivityTag_Activity();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.openregspecs.model.testing.Scenario <em>Scenario</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Scenario</em>'.
	 * @see org.eclipse.efbt.openregspecs.model.testing.Scenario
	 * @generated
	 */
	EClass getScenario();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.openregspecs.model.testing.Scenario#getName <em>Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Name</em>'.
	 * @see org.eclipse.efbt.openregspecs.model.testing.Scenario#getName()
	 * @see #getScenario()
	 * @generated
	 */
	EAttribute getScenario_Name();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.openregspecs.model.testing.Scenario#isInvisible <em>Invisible</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Invisible</em>'.
	 * @see org.eclipse.efbt.openregspecs.model.testing.Scenario#isInvisible()
	 * @see #getScenario()
	 * @generated
	 */
	EAttribute getScenario_Invisible();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.openregspecs.model.testing.Scenario#getDescription <em>Description</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Description</em>'.
	 * @see org.eclipse.efbt.openregspecs.model.testing.Scenario#getDescription()
	 * @see #getScenario()
	 * @generated
	 */
	EAttribute getScenario_Description();

	/**
	 * Returns the meta object for the reference list '{@link org.eclipse.efbt.openregspecs.model.testing.Scenario#getRequiredAttributes <em>Required Attributes</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference list '<em>Required Attributes</em>'.
	 * @see org.eclipse.efbt.openregspecs.model.testing.Scenario#getRequiredAttributes()
	 * @see #getScenario()
	 * @generated
	 */
	EReference getScenario_RequiredAttributes();

	/**
	 * Returns the meta object for the containment reference '{@link org.eclipse.efbt.openregspecs.model.testing.Scenario#getData_constraints <em>Data constraints</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Data constraints</em>'.
	 * @see org.eclipse.efbt.openregspecs.model.testing.Scenario#getData_constraints()
	 * @see #getScenario()
	 * @generated
	 */
	EReference getScenario_Data_constraints();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.openregspecs.model.testing.ScenarioTag <em>Scenario Tag</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Scenario Tag</em>'.
	 * @see org.eclipse.efbt.openregspecs.model.testing.ScenarioTag
	 * @generated
	 */
	EClass getScenarioTag();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.openregspecs.model.testing.ScenarioTag#getScenario <em>Scenario</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Scenario</em>'.
	 * @see org.eclipse.efbt.openregspecs.model.testing.ScenarioTag#getScenario()
	 * @see #getScenarioTag()
	 * @generated
	 */
	EReference getScenarioTag_Scenario();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.openregspecs.model.testing.Test <em>Test</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Test</em>'.
	 * @see org.eclipse.efbt.openregspecs.model.testing.Test
	 * @generated
	 */
	EClass getTest();

	/**
	 * Returns the meta object for the reference list '{@link org.eclipse.efbt.openregspecs.model.testing.Test#getInputData <em>Input Data</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference list '<em>Input Data</em>'.
	 * @see org.eclipse.efbt.openregspecs.model.testing.Test#getInputData()
	 * @see #getTest()
	 * @generated
	 */
	EReference getTest_InputData();

	/**
	 * Returns the meta object for the reference list '{@link org.eclipse.efbt.openregspecs.model.testing.Test#getExpectedResult <em>Expected Result</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference list '<em>Expected Result</em>'.
	 * @see org.eclipse.efbt.openregspecs.model.testing.Test#getExpectedResult()
	 * @see #getTest()
	 * @generated
	 */
	EReference getTest_ExpectedResult();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.openregspecs.model.testing.Test#getName <em>Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Name</em>'.
	 * @see org.eclipse.efbt.openregspecs.model.testing.Test#getName()
	 * @see #getTest()
	 * @generated
	 */
	EAttribute getTest_Name();

	/**
	 * Returns the meta object for the containment reference '{@link org.eclipse.efbt.openregspecs.model.testing.Test#getScope <em>Scope</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Scope</em>'.
	 * @see org.eclipse.efbt.openregspecs.model.testing.Test#getScope()
	 * @see #getTest()
	 * @generated
	 */
	EReference getTest_Scope();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.openregspecs.model.testing.TestModule <em>Test Module</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Test Module</em>'.
	 * @see org.eclipse.efbt.openregspecs.model.testing.TestModule
	 * @generated
	 */
	EClass getTestModule();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.openregspecs.model.testing.TestModule#getTests <em>Tests</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Tests</em>'.
	 * @see org.eclipse.efbt.openregspecs.model.testing.TestModule#getTests()
	 * @see #getTestModule()
	 * @generated
	 */
	EReference getTestModule_Tests();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.openregspecs.model.testing.DataConstraint <em>Data Constraint</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Data Constraint</em>'.
	 * @see org.eclipse.efbt.openregspecs.model.testing.DataConstraint
	 * @generated
	 */
	EClass getDataConstraint();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.openregspecs.model.testing.DataConstraint#getAttr1 <em>Attr1</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Attr1</em>'.
	 * @see org.eclipse.efbt.openregspecs.model.testing.DataConstraint#getAttr1()
	 * @see #getDataConstraint()
	 * @generated
	 */
	EReference getDataConstraint_Attr1();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.openregspecs.model.testing.DataConstraint#getAttr2 <em>Attr2</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Attr2</em>'.
	 * @see org.eclipse.efbt.openregspecs.model.testing.DataConstraint#getAttr2()
	 * @see #getDataConstraint()
	 * @generated
	 */
	EReference getDataConstraint_Attr2();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.openregspecs.model.testing.DataConstraint#getComparison <em>Comparison</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Comparison</em>'.
	 * @see org.eclipse.efbt.openregspecs.model.testing.DataConstraint#getComparison()
	 * @see #getDataConstraint()
	 * @generated
	 */
	EAttribute getDataConstraint_Comparison();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.openregspecs.model.testing.SelectionLayer <em>Selection Layer</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Selection Layer</em>'.
	 * @see org.eclipse.efbt.openregspecs.model.testing.SelectionLayer
	 * @generated
	 */
	EClass getSelectionLayer();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.openregspecs.model.testing.SelectionLayer#getName <em>Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Name</em>'.
	 * @see org.eclipse.efbt.openregspecs.model.testing.SelectionLayer#getName()
	 * @see #getSelectionLayer()
	 * @generated
	 */
	EAttribute getSelectionLayer_Name();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.openregspecs.model.testing.SelectionLayer#getInvisible <em>Invisible</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Invisible</em>'.
	 * @see org.eclipse.efbt.openregspecs.model.testing.SelectionLayer#getInvisible()
	 * @see #getSelectionLayer()
	 * @generated
	 */
	EAttribute getSelectionLayer_Invisible();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.openregspecs.model.testing.SelectionLayer#getGeneratedEntity <em>Generated Entity</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Generated Entity</em>'.
	 * @see org.eclipse.efbt.openregspecs.model.testing.SelectionLayer#getGeneratedEntity()
	 * @see #getSelectionLayer()
	 * @generated
	 */
	EReference getSelectionLayer_GeneratedEntity();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.openregspecs.model.testing.TestScope <em>Test Scope</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Test Scope</em>'.
	 * @see org.eclipse.efbt.openregspecs.model.testing.TestScope
	 * @generated
	 */
	EClass getTestScope();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.openregspecs.model.testing.TestScope#getName <em>Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Name</em>'.
	 * @see org.eclipse.efbt.openregspecs.model.testing.TestScope#getName()
	 * @see #getTestScope()
	 * @generated
	 */
	EAttribute getTestScope_Name();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.openregspecs.model.testing.UnitTestScope <em>Unit Test Scope</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Unit Test Scope</em>'.
	 * @see org.eclipse.efbt.openregspecs.model.testing.UnitTestScope
	 * @generated
	 */
	EClass getUnitTestScope();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.openregspecs.model.testing.UnitTestScope#getScenarios <em>Scenarios</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Scenarios</em>'.
	 * @see org.eclipse.efbt.openregspecs.model.testing.UnitTestScope#getScenarios()
	 * @see #getUnitTestScope()
	 * @generated
	 */
	EReference getUnitTestScope_Scenarios();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.openregspecs.model.testing.E2ETestScope <em>E2E Test Scope</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>E2E Test Scope</em>'.
	 * @see org.eclipse.efbt.openregspecs.model.testing.E2ETestScope
	 * @generated
	 */
	EClass getE2ETestScope();

	/**
	 * Returns the meta object for the reference list '{@link org.eclipse.efbt.openregspecs.model.testing.E2ETestScope#getScenarios <em>Scenarios</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference list '<em>Scenarios</em>'.
	 * @see org.eclipse.efbt.openregspecs.model.testing.E2ETestScope#getScenarios()
	 * @see #getE2ETestScope()
	 * @generated
	 */
	EReference getE2ETestScope_Scenarios();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.openregspecs.model.testing.E2ETestScope#getLayer <em>Layer</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Layer</em>'.
	 * @see org.eclipse.efbt.openregspecs.model.testing.E2ETestScope#getLayer()
	 * @see #getE2ETestScope()
	 * @generated
	 */
	EReference getE2ETestScope_Layer();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.openregspecs.model.testing.E2ETestScope#getScriptTask <em>Script Task</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Script Task</em>'.
	 * @see org.eclipse.efbt.openregspecs.model.testing.E2ETestScope#getScriptTask()
	 * @see #getE2ETestScope()
	 * @generated
	 */
	EReference getE2ETestScope_ScriptTask();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.openregspecs.model.testing.CSVFile <em>CSV File</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>CSV File</em>'.
	 * @see org.eclipse.efbt.openregspecs.model.testing.CSVFile
	 * @generated
	 */
	EClass getCSVFile();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.openregspecs.model.testing.CSVFile#getFileName <em>File Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>File Name</em>'.
	 * @see org.eclipse.efbt.openregspecs.model.testing.CSVFile#getFileName()
	 * @see #getCSVFile()
	 * @generated
	 */
	EAttribute getCSVFile_FileName();

	/**
	 * Returns the meta object for enum '{@link org.eclipse.efbt.openregspecs.model.testing.AttrComparison <em>Attr Comparison</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for enum '<em>Attr Comparison</em>'.
	 * @see org.eclipse.efbt.openregspecs.model.testing.AttrComparison
	 * @generated
	 */
	EEnum getAttrComparison();

	/**
	 * Returns the factory that creates the instances of the model.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the factory that creates the instances of the model.
	 * @generated
	 */
	TestingFactory getTestingFactory();

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
		 * The meta object literal for the '{@link org.eclipse.efbt.openregspecs.model.testing.impl.LogicalTransformationModuleImpl <em>Logical Transformation Module</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.openregspecs.model.testing.impl.LogicalTransformationModuleImpl
		 * @see org.eclipse.efbt.openregspecs.model.testing.impl.TestingPackageImpl#getLogicalTransformationModule()
		 * @generated
		 */
		EClass LOGICAL_TRANSFORMATION_MODULE = eINSTANCE.getLogicalTransformationModule();

		/**
		 * The meta object literal for the '<em><b>Task Tags</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference LOGICAL_TRANSFORMATION_MODULE__TASK_TAGS = eINSTANCE.getLogicalTransformationModule_TaskTags();

		/**
		 * The meta object literal for the '<em><b>Scenario Tags</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference LOGICAL_TRANSFORMATION_MODULE__SCENARIO_TAGS = eINSTANCE.getLogicalTransformationModule_ScenarioTags();

		/**
		 * The meta object literal for the '<em><b>Sub Process</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference LOGICAL_TRANSFORMATION_MODULE__SUB_PROCESS = eINSTANCE.getLogicalTransformationModule_SubProcess();

		/**
		 * The meta object literal for the '<em><b>Requirements</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference LOGICAL_TRANSFORMATION_MODULE__REQUIREMENTS = eINSTANCE.getLogicalTransformationModule_Requirements();

		/**
		 * The meta object literal for the '<em><b>Test Modules</b></em>' reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference LOGICAL_TRANSFORMATION_MODULE__TEST_MODULES = eINSTANCE.getLogicalTransformationModule_TestModules();

		/**
		 * The meta object literal for the '<em><b>Selection Layers</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference LOGICAL_TRANSFORMATION_MODULE__SELECTION_LAYERS = eINSTANCE.getLogicalTransformationModule_SelectionLayers();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.openregspecs.model.testing.impl.ActivityTagImpl <em>Activity Tag</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.openregspecs.model.testing.impl.ActivityTagImpl
		 * @see org.eclipse.efbt.openregspecs.model.testing.impl.TestingPackageImpl#getActivityTag()
		 * @generated
		 */
		EClass ACTIVITY_TAG = eINSTANCE.getActivityTag();

		/**
		 * The meta object literal for the '<em><b>Activity</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference ACTIVITY_TAG__ACTIVITY = eINSTANCE.getActivityTag_Activity();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.openregspecs.model.testing.impl.ScenarioImpl <em>Scenario</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.openregspecs.model.testing.impl.ScenarioImpl
		 * @see org.eclipse.efbt.openregspecs.model.testing.impl.TestingPackageImpl#getScenario()
		 * @generated
		 */
		EClass SCENARIO = eINSTANCE.getScenario();

		/**
		 * The meta object literal for the '<em><b>Name</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute SCENARIO__NAME = eINSTANCE.getScenario_Name();

		/**
		 * The meta object literal for the '<em><b>Invisible</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute SCENARIO__INVISIBLE = eINSTANCE.getScenario_Invisible();

		/**
		 * The meta object literal for the '<em><b>Description</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute SCENARIO__DESCRIPTION = eINSTANCE.getScenario_Description();

		/**
		 * The meta object literal for the '<em><b>Required Attributes</b></em>' reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference SCENARIO__REQUIRED_ATTRIBUTES = eINSTANCE.getScenario_RequiredAttributes();

		/**
		 * The meta object literal for the '<em><b>Data constraints</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference SCENARIO__DATA_CONSTRAINTS = eINSTANCE.getScenario_Data_constraints();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.openregspecs.model.testing.impl.ScenarioTagImpl <em>Scenario Tag</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.openregspecs.model.testing.impl.ScenarioTagImpl
		 * @see org.eclipse.efbt.openregspecs.model.testing.impl.TestingPackageImpl#getScenarioTag()
		 * @generated
		 */
		EClass SCENARIO_TAG = eINSTANCE.getScenarioTag();

		/**
		 * The meta object literal for the '<em><b>Scenario</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference SCENARIO_TAG__SCENARIO = eINSTANCE.getScenarioTag_Scenario();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.openregspecs.model.testing.impl.TestImpl <em>Test</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.openregspecs.model.testing.impl.TestImpl
		 * @see org.eclipse.efbt.openregspecs.model.testing.impl.TestingPackageImpl#getTest()
		 * @generated
		 */
		EClass TEST = eINSTANCE.getTest();

		/**
		 * The meta object literal for the '<em><b>Input Data</b></em>' reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference TEST__INPUT_DATA = eINSTANCE.getTest_InputData();

		/**
		 * The meta object literal for the '<em><b>Expected Result</b></em>' reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference TEST__EXPECTED_RESULT = eINSTANCE.getTest_ExpectedResult();

		/**
		 * The meta object literal for the '<em><b>Name</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute TEST__NAME = eINSTANCE.getTest_Name();

		/**
		 * The meta object literal for the '<em><b>Scope</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference TEST__SCOPE = eINSTANCE.getTest_Scope();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.openregspecs.model.testing.impl.TestModuleImpl <em>Test Module</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.openregspecs.model.testing.impl.TestModuleImpl
		 * @see org.eclipse.efbt.openregspecs.model.testing.impl.TestingPackageImpl#getTestModule()
		 * @generated
		 */
		EClass TEST_MODULE = eINSTANCE.getTestModule();

		/**
		 * The meta object literal for the '<em><b>Tests</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference TEST_MODULE__TESTS = eINSTANCE.getTestModule_Tests();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.openregspecs.model.testing.impl.DataConstraintImpl <em>Data Constraint</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.openregspecs.model.testing.impl.DataConstraintImpl
		 * @see org.eclipse.efbt.openregspecs.model.testing.impl.TestingPackageImpl#getDataConstraint()
		 * @generated
		 */
		EClass DATA_CONSTRAINT = eINSTANCE.getDataConstraint();

		/**
		 * The meta object literal for the '<em><b>Attr1</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference DATA_CONSTRAINT__ATTR1 = eINSTANCE.getDataConstraint_Attr1();

		/**
		 * The meta object literal for the '<em><b>Attr2</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference DATA_CONSTRAINT__ATTR2 = eINSTANCE.getDataConstraint_Attr2();

		/**
		 * The meta object literal for the '<em><b>Comparison</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute DATA_CONSTRAINT__COMPARISON = eINSTANCE.getDataConstraint_Comparison();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.openregspecs.model.testing.impl.SelectionLayerImpl <em>Selection Layer</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.openregspecs.model.testing.impl.SelectionLayerImpl
		 * @see org.eclipse.efbt.openregspecs.model.testing.impl.TestingPackageImpl#getSelectionLayer()
		 * @generated
		 */
		EClass SELECTION_LAYER = eINSTANCE.getSelectionLayer();

		/**
		 * The meta object literal for the '<em><b>Name</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute SELECTION_LAYER__NAME = eINSTANCE.getSelectionLayer_Name();

		/**
		 * The meta object literal for the '<em><b>Invisible</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute SELECTION_LAYER__INVISIBLE = eINSTANCE.getSelectionLayer_Invisible();

		/**
		 * The meta object literal for the '<em><b>Generated Entity</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference SELECTION_LAYER__GENERATED_ENTITY = eINSTANCE.getSelectionLayer_GeneratedEntity();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.openregspecs.model.testing.impl.TestScopeImpl <em>Test Scope</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.openregspecs.model.testing.impl.TestScopeImpl
		 * @see org.eclipse.efbt.openregspecs.model.testing.impl.TestingPackageImpl#getTestScope()
		 * @generated
		 */
		EClass TEST_SCOPE = eINSTANCE.getTestScope();

		/**
		 * The meta object literal for the '<em><b>Name</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute TEST_SCOPE__NAME = eINSTANCE.getTestScope_Name();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.openregspecs.model.testing.impl.UnitTestScopeImpl <em>Unit Test Scope</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.openregspecs.model.testing.impl.UnitTestScopeImpl
		 * @see org.eclipse.efbt.openregspecs.model.testing.impl.TestingPackageImpl#getUnitTestScope()
		 * @generated
		 */
		EClass UNIT_TEST_SCOPE = eINSTANCE.getUnitTestScope();

		/**
		 * The meta object literal for the '<em><b>Scenarios</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference UNIT_TEST_SCOPE__SCENARIOS = eINSTANCE.getUnitTestScope_Scenarios();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.openregspecs.model.testing.impl.E2ETestScopeImpl <em>E2E Test Scope</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.openregspecs.model.testing.impl.E2ETestScopeImpl
		 * @see org.eclipse.efbt.openregspecs.model.testing.impl.TestingPackageImpl#getE2ETestScope()
		 * @generated
		 */
		EClass E2E_TEST_SCOPE = eINSTANCE.getE2ETestScope();

		/**
		 * The meta object literal for the '<em><b>Scenarios</b></em>' reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference E2E_TEST_SCOPE__SCENARIOS = eINSTANCE.getE2ETestScope_Scenarios();

		/**
		 * The meta object literal for the '<em><b>Layer</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference E2E_TEST_SCOPE__LAYER = eINSTANCE.getE2ETestScope_Layer();

		/**
		 * The meta object literal for the '<em><b>Script Task</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference E2E_TEST_SCOPE__SCRIPT_TASK = eINSTANCE.getE2ETestScope_ScriptTask();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.openregspecs.model.testing.impl.CSVFileImpl <em>CSV File</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.openregspecs.model.testing.impl.CSVFileImpl
		 * @see org.eclipse.efbt.openregspecs.model.testing.impl.TestingPackageImpl#getCSVFile()
		 * @generated
		 */
		EClass CSV_FILE = eINSTANCE.getCSVFile();

		/**
		 * The meta object literal for the '<em><b>File Name</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute CSV_FILE__FILE_NAME = eINSTANCE.getCSVFile_FileName();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.openregspecs.model.testing.AttrComparison <em>Attr Comparison</em>}' enum.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.openregspecs.model.testing.AttrComparison
		 * @see org.eclipse.efbt.openregspecs.model.testing.impl.TestingPackageImpl#getAttrComparison()
		 * @generated
		 */
		EEnum ATTR_COMPARISON = eINSTANCE.getAttrComparison();

	}

} //TestingPackage
