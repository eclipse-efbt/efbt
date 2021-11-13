/**
 */
package org.eclipse.efbt.cocalimo.core.model.logical_transformations.impl;

import org.eclipse.efbt.cocalimo.core.model.bpmn_lite.Bpmn_litePackage;

import org.eclipse.efbt.cocalimo.core.model.bpmn_lite.impl.Bpmn_litePackageImpl;

import org.eclipse.efbt.cocalimo.core.model.logical_transformations.LogicalTransformationModule;
import org.eclipse.efbt.cocalimo.core.model.logical_transformations.Logical_transformationsFactory;
import org.eclipse.efbt.cocalimo.core.model.logical_transformations.Logical_transformationsPackage;
import org.eclipse.efbt.cocalimo.core.model.logical_transformations.Scenario;
import org.eclipse.efbt.cocalimo.core.model.logical_transformations.ScenarioTag;
import org.eclipse.efbt.cocalimo.core.model.logical_transformations.TaskTag;

import org.eclipse.efbt.cocalimo.core.model.logical_transformations.Test;
import org.eclipse.efbt.cocalimo.core.model.logical_transformations.TestModule;
import org.eclipse.efbt.cocalimo.core.model.module_management.Module_managementPackage;

import org.eclipse.efbt.cocalimo.core.model.requirements_text.Requirements_textPackage;
import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EPackage;
import org.eclipse.emf.ecore.EReference;
import org.eclipse.emf.ecore.EcorePackage;

import org.eclipse.emf.ecore.impl.EPackageImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model <b>Package</b>.
 * <!-- end-user-doc -->
 * @generated
 */
public class Logical_transformationsPackageImpl extends EPackageImpl implements Logical_transformationsPackage {
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
	private EClass taskTagEClass = null;

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
	 * @see org.eclipse.efbt.cocalimo.core.model.logical_transformations.Logical_transformationsPackage#eNS_URI
	 * @see #init()
	 * @generated
	 */
	private Logical_transformationsPackageImpl() {
		super(eNS_URI, Logical_transformationsFactory.eINSTANCE);
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
	 * <p>This method is used to initialize {@link Logical_transformationsPackage#eINSTANCE} when that field is accessed.
	 * Clients should not invoke it directly. Instead, they should simply access that field to obtain the package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #eNS_URI
	 * @see #createPackageContents()
	 * @see #initializePackageContents()
	 * @generated
	 */
	public static Logical_transformationsPackage init() {
		if (isInited) return (Logical_transformationsPackage)EPackage.Registry.INSTANCE.getEPackage(Logical_transformationsPackage.eNS_URI);

		// Obtain or create and register package
		Object registeredLogical_transformationsPackage = EPackage.Registry.INSTANCE.get(eNS_URI);
		Logical_transformationsPackageImpl theLogical_transformationsPackage = registeredLogical_transformationsPackage instanceof Logical_transformationsPackageImpl ? (Logical_transformationsPackageImpl)registeredLogical_transformationsPackage : new Logical_transformationsPackageImpl();

		isInited = true;

		// Initialize simple dependencies
		EcorePackage.eINSTANCE.eClass();
		Module_managementPackage.eINSTANCE.eClass();
		Requirements_textPackage.eINSTANCE.eClass();

		// Obtain or create and register interdependencies
		Object registeredPackage = EPackage.Registry.INSTANCE.getEPackage(Bpmn_litePackage.eNS_URI);
		Bpmn_litePackageImpl theBpmn_litePackage = (Bpmn_litePackageImpl)(registeredPackage instanceof Bpmn_litePackageImpl ? registeredPackage : Bpmn_litePackage.eINSTANCE);

		// Create package meta-data objects
		theLogical_transformationsPackage.createPackageContents();
		theBpmn_litePackage.createPackageContents();

		// Initialize created meta-data
		theLogical_transformationsPackage.initializePackageContents();
		theBpmn_litePackage.initializePackageContents();

		// Mark meta-data to indicate it can't be changed
		theLogical_transformationsPackage.freeze();

		// Update the registry and return the package
		EPackage.Registry.INSTANCE.put(Logical_transformationsPackage.eNS_URI, theLogical_transformationsPackage);
		return theLogical_transformationsPackage;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getLogicalTransformationModule() {
		return logicalTransformationModuleEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getLogicalTransformationModule_TaskTags() {
		return (EReference)logicalTransformationModuleEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getLogicalTransformationModule_ScenarioTags() {
		return (EReference)logicalTransformationModuleEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getLogicalTransformationModule_SubProcess() {
		return (EReference)logicalTransformationModuleEClass.getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getLogicalTransformationModule_SubProcessViews() {
		return (EReference)logicalTransformationModuleEClass.getEStructuralFeatures().get(3);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getLogicalTransformationModule_Requirements() {
		return (EReference)logicalTransformationModuleEClass.getEStructuralFeatures().get(4);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getLogicalTransformationModule_TestModules() {
		return (EReference)logicalTransformationModuleEClass.getEStructuralFeatures().get(5);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getTaskTag() {
		return taskTagEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getScenario() {
		return scenarioEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getScenario_Name() {
		return (EAttribute)scenarioEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getScenario_Invisible() {
		return (EAttribute)scenarioEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getScenario_Description() {
		return (EAttribute)scenarioEClass.getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getScenario_RequiredAttributes() {
		return (EReference)scenarioEClass.getEStructuralFeatures().get(3);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getScenarioTag() {
		return scenarioTagEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getScenarioTag_Scenario() {
		return (EReference)scenarioTagEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getTest() {
		return testEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getTest_Scenarios() {
		return (EReference)testEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getTest_InputData() {
		return (EReference)testEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getTest_ExpectedResult() {
		return (EReference)testEClass.getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getTestModule() {
		return testModuleEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getTestModule_Tests() {
		return (EReference)testModuleEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Logical_transformationsFactory getLogical_transformationsFactory() {
		return (Logical_transformationsFactory)getEFactoryInstance();
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
		createEReference(logicalTransformationModuleEClass, LOGICAL_TRANSFORMATION_MODULE__SUB_PROCESS_VIEWS);
		createEReference(logicalTransformationModuleEClass, LOGICAL_TRANSFORMATION_MODULE__REQUIREMENTS);
		createEReference(logicalTransformationModuleEClass, LOGICAL_TRANSFORMATION_MODULE__TEST_MODULES);

		taskTagEClass = createEClass(TASK_TAG);

		scenarioEClass = createEClass(SCENARIO);
		createEAttribute(scenarioEClass, SCENARIO__NAME);
		createEAttribute(scenarioEClass, SCENARIO__INVISIBLE);
		createEAttribute(scenarioEClass, SCENARIO__DESCRIPTION);
		createEReference(scenarioEClass, SCENARIO__REQUIRED_ATTRIBUTES);

		scenarioTagEClass = createEClass(SCENARIO_TAG);
		createEReference(scenarioTagEClass, SCENARIO_TAG__SCENARIO);

		testEClass = createEClass(TEST);
		createEReference(testEClass, TEST__SCENARIOS);
		createEReference(testEClass, TEST__INPUT_DATA);
		createEReference(testEClass, TEST__EXPECTED_RESULT);

		testModuleEClass = createEClass(TEST_MODULE);
		createEReference(testModuleEClass, TEST_MODULE__TESTS);
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
		EcorePackage theEcorePackage = (EcorePackage)EPackage.Registry.INSTANCE.getEPackage(EcorePackage.eNS_URI);

		// Create type parameters

		// Set bounds for type parameters

		// Add supertypes to classes
		logicalTransformationModuleEClass.getESuperTypes().add(theModule_managementPackage.getModule());
		taskTagEClass.getESuperTypes().add(theRequirements_textPackage.getTag());
		scenarioTagEClass.getESuperTypes().add(theRequirements_textPackage.getTag());
		testModuleEClass.getESuperTypes().add(theModule_managementPackage.getModule());

		// Initialize classes, features, and operations; add parameters
		initEClass(logicalTransformationModuleEClass, LogicalTransformationModule.class, "LogicalTransformationModule", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getLogicalTransformationModule_TaskTags(), this.getTaskTag(), null, "taskTags", null, 0, -1, LogicalTransformationModule.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getLogicalTransformationModule_ScenarioTags(), this.getScenarioTag(), null, "scenarioTags", null, 0, -1, LogicalTransformationModule.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getLogicalTransformationModule_SubProcess(), theBpmn_litePackage.getSubProcess(), null, "subProcess", null, 0, 1, LogicalTransformationModule.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getLogicalTransformationModule_SubProcessViews(), theBpmn_litePackage.getSubProcess(), null, "subProcessViews", null, 0, -1, LogicalTransformationModule.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getLogicalTransformationModule_Requirements(), theRequirements_textPackage.getRequirementsModule(), null, "requirements", null, 0, -1, LogicalTransformationModule.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getLogicalTransformationModule_TestModules(), this.getTestModule(), null, "testModules", null, 0, -1, LogicalTransformationModule.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(taskTagEClass, TaskTag.class, "TaskTag", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);

		initEClass(scenarioEClass, Scenario.class, "Scenario", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEAttribute(getScenario_Name(), ecorePackage.getEString(), "name", null, 0, 1, Scenario.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getScenario_Invisible(), ecorePackage.getEBoolean(), "invisible", null, 0, 1, Scenario.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getScenario_Description(), ecorePackage.getEString(), "description", null, 0, 1, Scenario.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getScenario_RequiredAttributes(), theEcorePackage.getEAttribute(), null, "requiredAttributes", null, 0, -1, Scenario.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(scenarioTagEClass, ScenarioTag.class, "ScenarioTag", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getScenarioTag_Scenario(), this.getScenario(), null, "scenario", null, 0, 1, ScenarioTag.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(testEClass, Test.class, "Test", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getTest_Scenarios(), this.getScenario(), null, "scenarios", null, 0, 1, Test.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getTest_InputData(), theEcorePackage.getEObject(), null, "inputData", null, 0, -1, Test.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getTest_ExpectedResult(), theEcorePackage.getEObject(), null, "expectedResult", null, 0, -1, Test.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(testModuleEClass, TestModule.class, "TestModule", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getTestModule_Tests(), this.getTest(), null, "tests", null, 0, -1, TestModule.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		// Create resource
		createResource(eNS_URI);

		// Create annotations
		// http:///org/eclipse/emf/ecore/util/ExtendedMetaData
		createExtendedMetaDataAnnotations();
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

} //Logical_transformationsPackageImpl
