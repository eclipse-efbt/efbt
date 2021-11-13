/**
 */
package org.eclipse.efbt.cocalimo.core.model.logical_transformations;

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
 * @see org.eclipse.efbt.cocalimo.core.model.logical_transformations.Logical_transformationsFactory
 * @model kind="package"
 * @generated
 */
public interface Logical_transformationsPackage extends EPackage {
	/**
	 * The package name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNAME = "logical_transformations";

	/**
	 * The package namespace URI.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNS_URI = "http://www.eclipse.org/efbt/logical_transformations";

	/**
	 * The package namespace name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNS_PREFIX = "logical_transformations";

	/**
	 * The singleton instance of the package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	Logical_transformationsPackage eINSTANCE = org.eclipse.efbt.cocalimo.core.model.logical_transformations.impl.Logical_transformationsPackageImpl.init();

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.core.model.logical_transformations.impl.LogicalTransformationModuleImpl <em>Logical Transformation Module</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.core.model.logical_transformations.impl.LogicalTransformationModuleImpl
	 * @see org.eclipse.efbt.cocalimo.core.model.logical_transformations.impl.Logical_transformationsPackageImpl#getLogicalTransformationModule()
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
	 * The feature id for the '<em><b>Sub Process Views</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int LOGICAL_TRANSFORMATION_MODULE__SUB_PROCESS_VIEWS = Module_managementPackage.MODULE_FEATURE_COUNT + 3;

	/**
	 * The feature id for the '<em><b>Requirements</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int LOGICAL_TRANSFORMATION_MODULE__REQUIREMENTS = Module_managementPackage.MODULE_FEATURE_COUNT + 4;

	/**
	 * The feature id for the '<em><b>Test Modules</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int LOGICAL_TRANSFORMATION_MODULE__TEST_MODULES = Module_managementPackage.MODULE_FEATURE_COUNT + 5;

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
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.core.model.logical_transformations.impl.TaskTagImpl <em>Task Tag</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.core.model.logical_transformations.impl.TaskTagImpl
	 * @see org.eclipse.efbt.cocalimo.core.model.logical_transformations.impl.Logical_transformationsPackageImpl#getTaskTag()
	 * @generated
	 */
	int TASK_TAG = 1;

	/**
	 * The feature id for the '<em><b>Display Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TASK_TAG__DISPLAY_NAME = Requirements_textPackage.TAG__DISPLAY_NAME;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TASK_TAG__NAME = Requirements_textPackage.TAG__NAME;

	/**
	 * The feature id for the '<em><b>Requirements</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TASK_TAG__REQUIREMENTS = Requirements_textPackage.TAG__REQUIREMENTS;

	/**
	 * The feature id for the '<em><b>Task</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TASK_TAG__TASK = Requirements_textPackage.TAG_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Task Tag</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TASK_TAG_FEATURE_COUNT = Requirements_textPackage.TAG_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Task Tag</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TASK_TAG_OPERATION_COUNT = Requirements_textPackage.TAG_OPERATION_COUNT + 0;


	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.core.model.logical_transformations.impl.ScenarioImpl <em>Scenario</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.core.model.logical_transformations.impl.ScenarioImpl
	 * @see org.eclipse.efbt.cocalimo.core.model.logical_transformations.impl.Logical_transformationsPackageImpl#getScenario()
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
	 * The number of structural features of the '<em>Scenario</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SCENARIO_FEATURE_COUNT = 4;

	/**
	 * The number of operations of the '<em>Scenario</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SCENARIO_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.core.model.logical_transformations.impl.ScenarioTagImpl <em>Scenario Tag</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.core.model.logical_transformations.impl.ScenarioTagImpl
	 * @see org.eclipse.efbt.cocalimo.core.model.logical_transformations.impl.Logical_transformationsPackageImpl#getScenarioTag()
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
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.core.model.logical_transformations.impl.TestImpl <em>Test</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.core.model.logical_transformations.impl.TestImpl
	 * @see org.eclipse.efbt.cocalimo.core.model.logical_transformations.impl.Logical_transformationsPackageImpl#getTest()
	 * @generated
	 */
	int TEST = 4;

	/**
	 * The feature id for the '<em><b>Scenarios</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TEST__SCENARIOS = 0;

	/**
	 * The feature id for the '<em><b>Input Data</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TEST__INPUT_DATA = 1;

	/**
	 * The feature id for the '<em><b>Expected Result</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TEST__EXPECTED_RESULT = 2;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TEST__NAME = 3;

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
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.core.model.logical_transformations.impl.TestModuleImpl <em>Test Module</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.core.model.logical_transformations.impl.TestModuleImpl
	 * @see org.eclipse.efbt.cocalimo.core.model.logical_transformations.impl.Logical_transformationsPackageImpl#getTestModule()
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
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.core.model.logical_transformations.LogicalTransformationModule <em>Logical Transformation Module</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Logical Transformation Module</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.logical_transformations.LogicalTransformationModule
	 * @generated
	 */
	EClass getLogicalTransformationModule();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.cocalimo.core.model.logical_transformations.LogicalTransformationModule#getTaskTags <em>Task Tags</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Task Tags</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.logical_transformations.LogicalTransformationModule#getTaskTags()
	 * @see #getLogicalTransformationModule()
	 * @generated
	 */
	EReference getLogicalTransformationModule_TaskTags();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.cocalimo.core.model.logical_transformations.LogicalTransformationModule#getScenarioTags <em>Scenario Tags</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Scenario Tags</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.logical_transformations.LogicalTransformationModule#getScenarioTags()
	 * @see #getLogicalTransformationModule()
	 * @generated
	 */
	EReference getLogicalTransformationModule_ScenarioTags();

	/**
	 * Returns the meta object for the containment reference '{@link org.eclipse.efbt.cocalimo.core.model.logical_transformations.LogicalTransformationModule#getSubProcess <em>Sub Process</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Sub Process</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.logical_transformations.LogicalTransformationModule#getSubProcess()
	 * @see #getLogicalTransformationModule()
	 * @generated
	 */
	EReference getLogicalTransformationModule_SubProcess();

	/**
	 * Returns the meta object for the reference list '{@link org.eclipse.efbt.cocalimo.core.model.logical_transformations.LogicalTransformationModule#getSubProcessViews <em>Sub Process Views</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference list '<em>Sub Process Views</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.logical_transformations.LogicalTransformationModule#getSubProcessViews()
	 * @see #getLogicalTransformationModule()
	 * @generated
	 */
	EReference getLogicalTransformationModule_SubProcessViews();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.cocalimo.core.model.logical_transformations.LogicalTransformationModule#getRequirements <em>Requirements</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Requirements</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.logical_transformations.LogicalTransformationModule#getRequirements()
	 * @see #getLogicalTransformationModule()
	 * @generated
	 */
	EReference getLogicalTransformationModule_Requirements();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.cocalimo.core.model.logical_transformations.LogicalTransformationModule#getTestModules <em>Test Modules</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Test Modules</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.logical_transformations.LogicalTransformationModule#getTestModules()
	 * @see #getLogicalTransformationModule()
	 * @generated
	 */
	EReference getLogicalTransformationModule_TestModules();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.core.model.logical_transformations.TaskTag <em>Task Tag</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Task Tag</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.logical_transformations.TaskTag
	 * @generated
	 */
	EClass getTaskTag();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.core.model.logical_transformations.TaskTag#getTask <em>Task</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Task</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.logical_transformations.TaskTag#getTask()
	 * @see #getTaskTag()
	 * @generated
	 */
	EReference getTaskTag_Task();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.core.model.logical_transformations.Scenario <em>Scenario</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Scenario</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.logical_transformations.Scenario
	 * @generated
	 */
	EClass getScenario();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.core.model.logical_transformations.Scenario#getName <em>Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Name</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.logical_transformations.Scenario#getName()
	 * @see #getScenario()
	 * @generated
	 */
	EAttribute getScenario_Name();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.core.model.logical_transformations.Scenario#isInvisible <em>Invisible</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Invisible</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.logical_transformations.Scenario#isInvisible()
	 * @see #getScenario()
	 * @generated
	 */
	EAttribute getScenario_Invisible();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.core.model.logical_transformations.Scenario#getDescription <em>Description</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Description</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.logical_transformations.Scenario#getDescription()
	 * @see #getScenario()
	 * @generated
	 */
	EAttribute getScenario_Description();

	/**
	 * Returns the meta object for the reference list '{@link org.eclipse.efbt.cocalimo.core.model.logical_transformations.Scenario#getRequiredAttributes <em>Required Attributes</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference list '<em>Required Attributes</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.logical_transformations.Scenario#getRequiredAttributes()
	 * @see #getScenario()
	 * @generated
	 */
	EReference getScenario_RequiredAttributes();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.core.model.logical_transformations.ScenarioTag <em>Scenario Tag</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Scenario Tag</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.logical_transformations.ScenarioTag
	 * @generated
	 */
	EClass getScenarioTag();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.core.model.logical_transformations.ScenarioTag#getScenario <em>Scenario</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Scenario</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.logical_transformations.ScenarioTag#getScenario()
	 * @see #getScenarioTag()
	 * @generated
	 */
	EReference getScenarioTag_Scenario();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.core.model.logical_transformations.Test <em>Test</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Test</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.logical_transformations.Test
	 * @generated
	 */
	EClass getTest();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.core.model.logical_transformations.Test#getScenarios <em>Scenarios</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Scenarios</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.logical_transformations.Test#getScenarios()
	 * @see #getTest()
	 * @generated
	 */
	EReference getTest_Scenarios();

	/**
	 * Returns the meta object for the reference list '{@link org.eclipse.efbt.cocalimo.core.model.logical_transformations.Test#getInputData <em>Input Data</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference list '<em>Input Data</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.logical_transformations.Test#getInputData()
	 * @see #getTest()
	 * @generated
	 */
	EReference getTest_InputData();

	/**
	 * Returns the meta object for the reference list '{@link org.eclipse.efbt.cocalimo.core.model.logical_transformations.Test#getExpectedResult <em>Expected Result</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference list '<em>Expected Result</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.logical_transformations.Test#getExpectedResult()
	 * @see #getTest()
	 * @generated
	 */
	EReference getTest_ExpectedResult();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.core.model.logical_transformations.Test#getName <em>Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Name</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.logical_transformations.Test#getName()
	 * @see #getTest()
	 * @generated
	 */
	EAttribute getTest_Name();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.core.model.logical_transformations.TestModule <em>Test Module</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Test Module</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.logical_transformations.TestModule
	 * @generated
	 */
	EClass getTestModule();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.cocalimo.core.model.logical_transformations.TestModule#getTests <em>Tests</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Tests</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.logical_transformations.TestModule#getTests()
	 * @see #getTestModule()
	 * @generated
	 */
	EReference getTestModule_Tests();

	/**
	 * Returns the factory that creates the instances of the model.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the factory that creates the instances of the model.
	 * @generated
	 */
	Logical_transformationsFactory getLogical_transformationsFactory();

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
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.core.model.logical_transformations.impl.LogicalTransformationModuleImpl <em>Logical Transformation Module</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.core.model.logical_transformations.impl.LogicalTransformationModuleImpl
		 * @see org.eclipse.efbt.cocalimo.core.model.logical_transformations.impl.Logical_transformationsPackageImpl#getLogicalTransformationModule()
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
		 * The meta object literal for the '<em><b>Sub Process Views</b></em>' reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference LOGICAL_TRANSFORMATION_MODULE__SUB_PROCESS_VIEWS = eINSTANCE.getLogicalTransformationModule_SubProcessViews();

		/**
		 * The meta object literal for the '<em><b>Requirements</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference LOGICAL_TRANSFORMATION_MODULE__REQUIREMENTS = eINSTANCE.getLogicalTransformationModule_Requirements();

		/**
		 * The meta object literal for the '<em><b>Test Modules</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference LOGICAL_TRANSFORMATION_MODULE__TEST_MODULES = eINSTANCE.getLogicalTransformationModule_TestModules();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.core.model.logical_transformations.impl.TaskTagImpl <em>Task Tag</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.core.model.logical_transformations.impl.TaskTagImpl
		 * @see org.eclipse.efbt.cocalimo.core.model.logical_transformations.impl.Logical_transformationsPackageImpl#getTaskTag()
		 * @generated
		 */
		EClass TASK_TAG = eINSTANCE.getTaskTag();

		/**
		 * The meta object literal for the '<em><b>Task</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference TASK_TAG__TASK = eINSTANCE.getTaskTag_Task();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.core.model.logical_transformations.impl.ScenarioImpl <em>Scenario</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.core.model.logical_transformations.impl.ScenarioImpl
		 * @see org.eclipse.efbt.cocalimo.core.model.logical_transformations.impl.Logical_transformationsPackageImpl#getScenario()
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
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.core.model.logical_transformations.impl.ScenarioTagImpl <em>Scenario Tag</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.core.model.logical_transformations.impl.ScenarioTagImpl
		 * @see org.eclipse.efbt.cocalimo.core.model.logical_transformations.impl.Logical_transformationsPackageImpl#getScenarioTag()
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
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.core.model.logical_transformations.impl.TestImpl <em>Test</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.core.model.logical_transformations.impl.TestImpl
		 * @see org.eclipse.efbt.cocalimo.core.model.logical_transformations.impl.Logical_transformationsPackageImpl#getTest()
		 * @generated
		 */
		EClass TEST = eINSTANCE.getTest();

		/**
		 * The meta object literal for the '<em><b>Scenarios</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference TEST__SCENARIOS = eINSTANCE.getTest_Scenarios();

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
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.core.model.logical_transformations.impl.TestModuleImpl <em>Test Module</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.core.model.logical_transformations.impl.TestModuleImpl
		 * @see org.eclipse.efbt.cocalimo.core.model.logical_transformations.impl.Logical_transformationsPackageImpl#getTestModule()
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

	}

} //Logical_transformationsPackage
