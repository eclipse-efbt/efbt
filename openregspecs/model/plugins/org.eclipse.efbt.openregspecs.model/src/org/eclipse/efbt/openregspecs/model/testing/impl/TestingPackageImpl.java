/**
 */
package org.eclipse.efbt.openregspecs.model.testing.impl;

import org.eclipse.efbt.openregspecs.model.bpmn_lite.Bpmn_litePackage;

import org.eclipse.efbt.openregspecs.model.bpmn_lite.impl.Bpmn_litePackageImpl;

import org.eclipse.efbt.openregspecs.model.data_meta_model.Data_meta_modelPackage;

import org.eclipse.efbt.openregspecs.model.data_meta_model.impl.Data_meta_modelPackageImpl;

import org.eclipse.efbt.openregspecs.model.module_management.Module_managementPackage;

import org.eclipse.efbt.openregspecs.model.module_management.impl.Module_managementPackageImpl;

import org.eclipse.efbt.openregspecs.model.requirements_text.Requirements_textPackage;

import org.eclipse.efbt.openregspecs.model.requirements_text.impl.Requirements_textPackageImpl;

import org.eclipse.efbt.openregspecs.model.sql_lite.Sql_litePackage;

import org.eclipse.efbt.openregspecs.model.sql_lite.impl.Sql_litePackageImpl;

import org.eclipse.efbt.openregspecs.model.testing.ActivityTag;
import org.eclipse.efbt.openregspecs.model.testing.AttrComparison;
import org.eclipse.efbt.openregspecs.model.testing.CSVFile;
import org.eclipse.efbt.openregspecs.model.testing.DataConstraint;
import org.eclipse.efbt.openregspecs.model.testing.E2ETestScope;
import org.eclipse.efbt.openregspecs.model.testing.LogicalTransformationModule;
import org.eclipse.efbt.openregspecs.model.testing.Scenario;
import org.eclipse.efbt.openregspecs.model.testing.ScenarioTag;
import org.eclipse.efbt.openregspecs.model.testing.SelectionLayer;
import org.eclipse.efbt.openregspecs.model.testing.Test;
import org.eclipse.efbt.openregspecs.model.testing.TestModule;
import org.eclipse.efbt.openregspecs.model.testing.TestScope;
import org.eclipse.efbt.openregspecs.model.testing.TestingFactory;
import org.eclipse.efbt.openregspecs.model.testing.TestingPackage;
import org.eclipse.efbt.openregspecs.model.testing.UnitTestScope;

import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EEnum;
import org.eclipse.emf.ecore.EPackage;
import org.eclipse.emf.ecore.EReference;

import org.eclipse.emf.ecore.impl.EPackageImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model <b>Package</b>.
 * <!-- end-user-doc -->
 * @generated
 */
public class TestingPackageImpl extends EPackageImpl implements TestingPackage {
	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass logicalTransformationModuleEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass activityTagEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass scenarioEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass scenarioTagEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass testEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass testModuleEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass dataConstraintEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass selectionLayerEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass testScopeEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass unitTestScopeEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass e2ETestScopeEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass csvFileEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EEnum attrComparisonEEnum = null;

	/**
	 * Creates an instance of the model <b>Package</b>, registered with
	 * {@link org.eclipse.emf.ecore.EPackage.Registry EPackage.Registry} by the package
	 * package URI value.
	 * <p>Note: the correct way to create the package is via the static
	 * factory method {@link #init init()}, which also performs
	 * initialization of the package, or returns the registered package,
	 * if one already exists.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.emf.ecore.EPackage.Registry
	 * @see org.eclipse.efbt.openregspecs.model.testing.TestingPackage#eNS_URI
	 * @see #init()
	 * @generated
	 */
	private TestingPackageImpl() {
		super(eNS_URI, TestingFactory.eINSTANCE);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private static boolean isInited = false;

	/**
	 * Creates, registers, and initializes the <b>Package</b> for this model, and for any others upon which it depends.
	 *
	 * <p>This method is used to initialize {@link TestingPackage#eINSTANCE} when that field is accessed.
	 * Clients should not invoke it directly. Instead, they should simply access that field to obtain the package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #eNS_URI
	 * @see #createPackageContents()
	 * @see #initializePackageContents()
	 * @generated
	 */
	public static TestingPackage init() {
		if (isInited) return (TestingPackage)EPackage.Registry.INSTANCE.getEPackage(TestingPackage.eNS_URI);

		// Obtain or create and register package
		Object registeredTestingPackage = EPackage.Registry.INSTANCE.get(eNS_URI);
		TestingPackageImpl theTestingPackage = registeredTestingPackage instanceof TestingPackageImpl ? (TestingPackageImpl)registeredTestingPackage : new TestingPackageImpl();

		isInited = true;

		// Obtain or create and register interdependencies
		Object registeredPackage = EPackage.Registry.INSTANCE.getEPackage(Bpmn_litePackage.eNS_URI);
		Bpmn_litePackageImpl theBpmn_litePackage = (Bpmn_litePackageImpl)(registeredPackage instanceof Bpmn_litePackageImpl ? registeredPackage : Bpmn_litePackage.eINSTANCE);
		registeredPackage = EPackage.Registry.INSTANCE.getEPackage(Data_meta_modelPackage.eNS_URI);
		Data_meta_modelPackageImpl theData_meta_modelPackage = (Data_meta_modelPackageImpl)(registeredPackage instanceof Data_meta_modelPackageImpl ? registeredPackage : Data_meta_modelPackage.eINSTANCE);
		registeredPackage = EPackage.Registry.INSTANCE.getEPackage(Module_managementPackage.eNS_URI);
		Module_managementPackageImpl theModule_managementPackage = (Module_managementPackageImpl)(registeredPackage instanceof Module_managementPackageImpl ? registeredPackage : Module_managementPackage.eINSTANCE);
		registeredPackage = EPackage.Registry.INSTANCE.getEPackage(Requirements_textPackage.eNS_URI);
		Requirements_textPackageImpl theRequirements_textPackage = (Requirements_textPackageImpl)(registeredPackage instanceof Requirements_textPackageImpl ? registeredPackage : Requirements_textPackage.eINSTANCE);
		registeredPackage = EPackage.Registry.INSTANCE.getEPackage(Sql_litePackage.eNS_URI);
		Sql_litePackageImpl theSql_litePackage = (Sql_litePackageImpl)(registeredPackage instanceof Sql_litePackageImpl ? registeredPackage : Sql_litePackage.eINSTANCE);

		// Create package meta-data objects
		theTestingPackage.createPackageContents();
		theBpmn_litePackage.createPackageContents();
		theData_meta_modelPackage.createPackageContents();
		theModule_managementPackage.createPackageContents();
		theRequirements_textPackage.createPackageContents();
		theSql_litePackage.createPackageContents();

		// Initialize created meta-data
		theTestingPackage.initializePackageContents();
		theBpmn_litePackage.initializePackageContents();
		theData_meta_modelPackage.initializePackageContents();
		theModule_managementPackage.initializePackageContents();
		theRequirements_textPackage.initializePackageContents();
		theSql_litePackage.initializePackageContents();

		// Mark meta-data to indicate it can't be changed
		theTestingPackage.freeze();

		// Update the registry and return the package
		EPackage.Registry.INSTANCE.put(TestingPackage.eNS_URI, theTestingPackage);
		return theTestingPackage;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getLogicalTransformationModule() {
		return logicalTransformationModuleEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getLogicalTransformationModule_TaskTags() {
		return (EReference)logicalTransformationModuleEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getLogicalTransformationModule_ScenarioTags() {
		return (EReference)logicalTransformationModuleEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getLogicalTransformationModule_SubProcess() {
		return (EReference)logicalTransformationModuleEClass.getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getLogicalTransformationModule_Requirements() {
		return (EReference)logicalTransformationModuleEClass.getEStructuralFeatures().get(3);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getLogicalTransformationModule_TestModules() {
		return (EReference)logicalTransformationModuleEClass.getEStructuralFeatures().get(4);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getLogicalTransformationModule_SelectionLayers() {
		return (EReference)logicalTransformationModuleEClass.getEStructuralFeatures().get(5);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getActivityTag() {
		return activityTagEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getActivityTag_Activity() {
		return (EReference)activityTagEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getScenario() {
		return scenarioEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getScenario_Name() {
		return (EAttribute)scenarioEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getScenario_Invisible() {
		return (EAttribute)scenarioEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getScenario_Description() {
		return (EAttribute)scenarioEClass.getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getScenario_RequiredAttributes() {
		return (EReference)scenarioEClass.getEStructuralFeatures().get(3);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getScenario_Data_constraints() {
		return (EReference)scenarioEClass.getEStructuralFeatures().get(4);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getScenarioTag() {
		return scenarioTagEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getScenarioTag_Scenario() {
		return (EReference)scenarioTagEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getTest() {
		return testEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getTest_InputData() {
		return (EReference)testEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getTest_ExpectedResult() {
		return (EReference)testEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getTest_Name() {
		return (EAttribute)testEClass.getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getTest_Scope() {
		return (EReference)testEClass.getEStructuralFeatures().get(3);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getTestModule() {
		return testModuleEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getTestModule_Tests() {
		return (EReference)testModuleEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getDataConstraint() {
		return dataConstraintEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getDataConstraint_Attr1() {
		return (EReference)dataConstraintEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getDataConstraint_Attr2() {
		return (EReference)dataConstraintEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getDataConstraint_Comparison() {
		return (EAttribute)dataConstraintEClass.getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getSelectionLayer() {
		return selectionLayerEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getSelectionLayer_Name() {
		return (EAttribute)selectionLayerEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getSelectionLayer_Invisible() {
		return (EAttribute)selectionLayerEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getSelectionLayer_GeneratedEntity() {
		return (EReference)selectionLayerEClass.getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getTestScope() {
		return testScopeEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getTestScope_Name() {
		return (EAttribute)testScopeEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getUnitTestScope() {
		return unitTestScopeEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getUnitTestScope_Scenarios() {
		return (EReference)unitTestScopeEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getE2ETestScope() {
		return e2ETestScopeEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getE2ETestScope_Scenarios() {
		return (EReference)e2ETestScopeEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getE2ETestScope_Layer() {
		return (EReference)e2ETestScopeEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getE2ETestScope_ScriptTask() {
		return (EReference)e2ETestScopeEClass.getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getCSVFile() {
		return csvFileEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getCSVFile_FileName() {
		return (EAttribute)csvFileEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EEnum getAttrComparison() {
		return attrComparisonEEnum;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public TestingFactory getTestingFactory() {
		return (TestingFactory)getEFactoryInstance();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private boolean isCreated = false;

	/**
	 * Creates the meta-model objects for the package.  This method is
	 * guarded to have no affect on any invocation but its first.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void createPackageContents() {
		if (isCreated) return;
		isCreated = true;

		// Create classes and their features
		logicalTransformationModuleEClass = createEClass(LOGICAL_TRANSFORMATION_MODULE);
		createEReference(logicalTransformationModuleEClass, LOGICAL_TRANSFORMATION_MODULE__TASK_TAGS);
		createEReference(logicalTransformationModuleEClass, LOGICAL_TRANSFORMATION_MODULE__SCENARIO_TAGS);
		createEReference(logicalTransformationModuleEClass, LOGICAL_TRANSFORMATION_MODULE__SUB_PROCESS);
		createEReference(logicalTransformationModuleEClass, LOGICAL_TRANSFORMATION_MODULE__REQUIREMENTS);
		createEReference(logicalTransformationModuleEClass, LOGICAL_TRANSFORMATION_MODULE__TEST_MODULES);
		createEReference(logicalTransformationModuleEClass, LOGICAL_TRANSFORMATION_MODULE__SELECTION_LAYERS);

		activityTagEClass = createEClass(ACTIVITY_TAG);
		createEReference(activityTagEClass, ACTIVITY_TAG__ACTIVITY);

		scenarioEClass = createEClass(SCENARIO);
		createEAttribute(scenarioEClass, SCENARIO__NAME);
		createEAttribute(scenarioEClass, SCENARIO__INVISIBLE);
		createEAttribute(scenarioEClass, SCENARIO__DESCRIPTION);
		createEReference(scenarioEClass, SCENARIO__REQUIRED_ATTRIBUTES);
		createEReference(scenarioEClass, SCENARIO__DATA_CONSTRAINTS);

		scenarioTagEClass = createEClass(SCENARIO_TAG);
		createEReference(scenarioTagEClass, SCENARIO_TAG__SCENARIO);

		testEClass = createEClass(TEST);
		createEReference(testEClass, TEST__INPUT_DATA);
		createEReference(testEClass, TEST__EXPECTED_RESULT);
		createEAttribute(testEClass, TEST__NAME);
		createEReference(testEClass, TEST__SCOPE);

		testModuleEClass = createEClass(TEST_MODULE);
		createEReference(testModuleEClass, TEST_MODULE__TESTS);

		dataConstraintEClass = createEClass(DATA_CONSTRAINT);
		createEReference(dataConstraintEClass, DATA_CONSTRAINT__ATTR1);
		createEReference(dataConstraintEClass, DATA_CONSTRAINT__ATTR2);
		createEAttribute(dataConstraintEClass, DATA_CONSTRAINT__COMPARISON);

		selectionLayerEClass = createEClass(SELECTION_LAYER);
		createEAttribute(selectionLayerEClass, SELECTION_LAYER__NAME);
		createEAttribute(selectionLayerEClass, SELECTION_LAYER__INVISIBLE);
		createEReference(selectionLayerEClass, SELECTION_LAYER__GENERATED_ENTITY);

		testScopeEClass = createEClass(TEST_SCOPE);
		createEAttribute(testScopeEClass, TEST_SCOPE__NAME);

		unitTestScopeEClass = createEClass(UNIT_TEST_SCOPE);
		createEReference(unitTestScopeEClass, UNIT_TEST_SCOPE__SCENARIOS);

		e2ETestScopeEClass = createEClass(E2E_TEST_SCOPE);
		createEReference(e2ETestScopeEClass, E2E_TEST_SCOPE__SCENARIOS);
		createEReference(e2ETestScopeEClass, E2E_TEST_SCOPE__LAYER);
		createEReference(e2ETestScopeEClass, E2E_TEST_SCOPE__SCRIPT_TASK);

		csvFileEClass = createEClass(CSV_FILE);
		createEAttribute(csvFileEClass, CSV_FILE__FILE_NAME);

		// Create enums
		attrComparisonEEnum = createEEnum(ATTR_COMPARISON);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private boolean isInitialized = false;

	/**
	 * Complete the initialization of the package and its meta-model.  This
	 * method is guarded to have no affect on any invocation but its first.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void initializePackageContents() {
		if (isInitialized) return;
		isInitialized = true;

		// Initialize package
		setName(eNAME);
		setNsPrefix(eNS_PREFIX);
		setNsURI(eNS_URI);

		// Obtain other dependent packages
		Module_managementPackage theModule_managementPackage = (Module_managementPackage)EPackage.Registry.INSTANCE.getEPackage(Module_managementPackage.eNS_URI);
		Bpmn_litePackage theBpmn_litePackage = (Bpmn_litePackage)EPackage.Registry.INSTANCE.getEPackage(Bpmn_litePackage.eNS_URI);
		Requirements_textPackage theRequirements_textPackage = (Requirements_textPackage)EPackage.Registry.INSTANCE.getEPackage(Requirements_textPackage.eNS_URI);
		Data_meta_modelPackage theData_meta_modelPackage = (Data_meta_modelPackage)EPackage.Registry.INSTANCE.getEPackage(Data_meta_modelPackage.eNS_URI);

		// Create type parameters

		// Set bounds for type parameters

		// Add supertypes to classes
		logicalTransformationModuleEClass.getESuperTypes().add(theModule_managementPackage.getModule());
		activityTagEClass.getESuperTypes().add(theRequirements_textPackage.getTag());
		scenarioTagEClass.getESuperTypes().add(theRequirements_textPackage.getTag());
		testModuleEClass.getESuperTypes().add(theModule_managementPackage.getModule());
		unitTestScopeEClass.getESuperTypes().add(this.getTestScope());
		e2ETestScopeEClass.getESuperTypes().add(this.getTestScope());

		// Initialize classes, features, and operations; add parameters
		initEClass(logicalTransformationModuleEClass, LogicalTransformationModule.class, "LogicalTransformationModule", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getLogicalTransformationModule_TaskTags(), this.getActivityTag(), null, "taskTags", null, 0, -1, LogicalTransformationModule.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getLogicalTransformationModule_ScenarioTags(), this.getScenarioTag(), null, "scenarioTags", null, 0, -1, LogicalTransformationModule.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getLogicalTransformationModule_SubProcess(), theBpmn_litePackage.getSubProcess(), null, "subProcess", null, 0, 1, LogicalTransformationModule.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getLogicalTransformationModule_Requirements(), theRequirements_textPackage.getRequirementsModule(), null, "requirements", null, 0, -1, LogicalTransformationModule.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getLogicalTransformationModule_TestModules(), this.getTestModule(), null, "testModules", null, 0, -1, LogicalTransformationModule.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getLogicalTransformationModule_SelectionLayers(), this.getSelectionLayer(), null, "selectionLayers", null, 0, -1, LogicalTransformationModule.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(activityTagEClass, ActivityTag.class, "ActivityTag", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getActivityTag_Activity(), theBpmn_litePackage.getActivity(), null, "activity", null, 0, 1, ActivityTag.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(scenarioEClass, Scenario.class, "Scenario", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEAttribute(getScenario_Name(), ecorePackage.getEString(), "name", null, 0, 1, Scenario.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getScenario_Invisible(), ecorePackage.getEBoolean(), "invisible", null, 0, 1, Scenario.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getScenario_Description(), ecorePackage.getEString(), "description", null, 0, 1, Scenario.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getScenario_RequiredAttributes(), theData_meta_modelPackage.getAttribute(), null, "requiredAttributes", null, 0, -1, Scenario.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getScenario_Data_constraints(), this.getDataConstraint(), null, "data_constraints", null, 0, 1, Scenario.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(scenarioTagEClass, ScenarioTag.class, "ScenarioTag", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getScenarioTag_Scenario(), this.getScenario(), null, "scenario", null, 0, 1, ScenarioTag.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(testEClass, Test.class, "Test", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getTest_InputData(), this.getCSVFile(), null, "inputData", null, 0, -1, Test.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getTest_ExpectedResult(), this.getCSVFile(), null, "expectedResult", null, 0, -1, Test.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getTest_Name(), ecorePackage.getEString(), "name", null, 0, 1, Test.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getTest_Scope(), this.getTestScope(), null, "scope", null, 0, 1, Test.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(testModuleEClass, TestModule.class, "TestModule", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getTestModule_Tests(), this.getTest(), null, "tests", null, 0, -1, TestModule.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(dataConstraintEClass, DataConstraint.class, "DataConstraint", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getDataConstraint_Attr1(), theData_meta_modelPackage.getAttribute(), null, "attr1", null, 0, 1, DataConstraint.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getDataConstraint_Attr2(), theData_meta_modelPackage.getAttribute(), null, "attr2", null, 0, 1, DataConstraint.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getDataConstraint_Comparison(), this.getAttrComparison(), "comparison", null, 0, 1, DataConstraint.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(selectionLayerEClass, SelectionLayer.class, "SelectionLayer", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEAttribute(getSelectionLayer_Name(), ecorePackage.getEString(), "name", null, 0, 1, SelectionLayer.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getSelectionLayer_Invisible(), ecorePackage.getEBooleanObject(), "invisible", null, 0, 1, SelectionLayer.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getSelectionLayer_GeneratedEntity(), theData_meta_modelPackage.getGeneratedEntity(), null, "generatedEntity", null, 0, 1, SelectionLayer.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(testScopeEClass, TestScope.class, "TestScope", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEAttribute(getTestScope_Name(), ecorePackage.getEString(), "name", null, 0, 1, TestScope.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(unitTestScopeEClass, UnitTestScope.class, "UnitTestScope", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getUnitTestScope_Scenarios(), this.getScenario(), null, "scenarios", null, 0, 1, UnitTestScope.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(e2ETestScopeEClass, E2ETestScope.class, "E2ETestScope", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getE2ETestScope_Scenarios(), this.getScenario(), null, "scenarios", null, 0, -1, E2ETestScope.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getE2ETestScope_Layer(), this.getSelectionLayer(), null, "layer", null, 0, 1, E2ETestScope.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getE2ETestScope_ScriptTask(), theBpmn_litePackage.getScriptTask(), null, "scriptTask", null, 0, 1, E2ETestScope.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(csvFileEClass, CSVFile.class, "CSVFile", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEAttribute(getCSVFile_FileName(), ecorePackage.getEString(), "fileName", null, 0, 1, CSVFile.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		// Initialize enums and add enum literals
		initEEnum(attrComparisonEEnum, AttrComparison.class, "AttrComparison");
		addEEnumLiteral(attrComparisonEEnum, AttrComparison.EQUALS);
		addEEnumLiteral(attrComparisonEEnum, AttrComparison.LESS_THAN);
		addEEnumLiteral(attrComparisonEEnum, AttrComparison.GREATER_THAN);

		// Create resource
		createResource(eNS_URI);

		// Create annotations
		// license
		createLicenseAnnotations();
		// http:///org/eclipse/emf/ecore/util/ExtendedMetaData
		createExtendedMetaDataAnnotations();
	}

	/**
	 * Initializes the annotations for <b>license</b>.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected void createLicenseAnnotations() {
		String source = "license";
		addAnnotation
		  (this,
		   source,
		   new String[] {
			   "license", "Copyright (c) 2020 Bird Software Solutions Ltd\n All rights reserved. This file and the accompanying materials are made available under the terms of the Eclipse Public License v2.0 which accompanies this distribution, and is available at http://www.eclipse.org/legal/epl-v20.html\n\nSPDX-License-Identifier: EPL-2.0 \n\n Contributors:\n Neil Mackenzie - initial API and implementation\r"
		   });
	}

	/**
	 * Initializes the annotations for <b>http:///org/eclipse/emf/ecore/util/ExtendedMetaData</b>.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected void createExtendedMetaDataAnnotations() {
		String source = "http:///org/eclipse/emf/ecore/util/ExtendedMetaData";
		addAnnotation
		  (scenarioEClass,
		   source,
		   new String[] {
			   "name", "Scenario",
			   "kind", "empty"
		   });
		addAnnotation
		  (getScenario_Name(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "name"
		   });
		addAnnotation
		  (getScenario_Description(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "description"
		   });
	}

} //TestingPackageImpl
