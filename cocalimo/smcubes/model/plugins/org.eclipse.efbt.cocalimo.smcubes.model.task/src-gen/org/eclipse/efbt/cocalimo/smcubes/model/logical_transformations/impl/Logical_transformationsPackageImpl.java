/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.logical_transformations.impl;

import org.eclipse.efbt.cocalimo.core.model.module_management.Module_managementPackage;

import org.eclipse.efbt.cocalimo.core.model.requirements_text.Requirements_textPackage;

import org.eclipse.efbt.cocalimo.smcubes.model.bpmn_lite.Bpmn_litePackage;

import org.eclipse.efbt.cocalimo.smcubes.model.bpmn_lite.impl.Bpmn_litePackageImpl;

import org.eclipse.efbt.cocalimo.smcubes.model.core.CorePackage;

import org.eclipse.efbt.cocalimo.smcubes.model.data_definition.Data_definitionPackage;

import org.eclipse.efbt.cocalimo.smcubes.model.logical_transformations.LogicalTransformationModule;
import org.eclipse.efbt.cocalimo.smcubes.model.logical_transformations.Logical_transformationsFactory;
import org.eclipse.efbt.cocalimo.smcubes.model.logical_transformations.Logical_transformationsPackage;
import org.eclipse.efbt.cocalimo.smcubes.model.logical_transformations.TaskTag;

import org.eclipse.efbt.cocalimo.smcubes.model.scenarios.ScenariosPackage;

import org.eclipse.efbt.cocalimo.smcubes.model.scenarios.impl.ScenariosPackageImpl;

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
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.logical_transformations.Logical_transformationsPackage#eNS_URI
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
		CorePackage.eINSTANCE.eClass();
		Data_definitionPackage.eINSTANCE.eClass();

		// Obtain or create and register interdependencies
		Object registeredPackage = EPackage.Registry.INSTANCE.getEPackage(ScenariosPackage.eNS_URI);
		ScenariosPackageImpl theScenariosPackage = (ScenariosPackageImpl)(registeredPackage instanceof ScenariosPackageImpl ? registeredPackage : ScenariosPackage.eINSTANCE);
		registeredPackage = EPackage.Registry.INSTANCE.getEPackage(Bpmn_litePackage.eNS_URI);
		Bpmn_litePackageImpl theBpmn_litePackage = (Bpmn_litePackageImpl)(registeredPackage instanceof Bpmn_litePackageImpl ? registeredPackage : Bpmn_litePackage.eINSTANCE);

		// Create package meta-data objects
		theLogical_transformationsPackage.createPackageContents();
		theScenariosPackage.createPackageContents();
		theBpmn_litePackage.createPackageContents();

		// Initialize created meta-data
		theLogical_transformationsPackage.initializePackageContents();
		theScenariosPackage.initializePackageContents();
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
	public EReference getLogicalTransformationModule_ScenarioSetTags() {
		return (EReference)logicalTransformationModuleEClass.getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getLogicalTransformationModule_SubProcesses() {
		return (EReference)logicalTransformationModuleEClass.getEStructuralFeatures().get(3);
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
		createEReference(logicalTransformationModuleEClass, LOGICAL_TRANSFORMATION_MODULE__SCENARIO_SET_TAGS);
		createEReference(logicalTransformationModuleEClass, LOGICAL_TRANSFORMATION_MODULE__SUB_PROCESSES);

		taskTagEClass = createEClass(TASK_TAG);
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
		ScenariosPackage theScenariosPackage = (ScenariosPackage)EPackage.Registry.INSTANCE.getEPackage(ScenariosPackage.eNS_URI);
		Bpmn_litePackage theBpmn_litePackage = (Bpmn_litePackage)EPackage.Registry.INSTANCE.getEPackage(Bpmn_litePackage.eNS_URI);
		Requirements_textPackage theRequirements_textPackage = (Requirements_textPackage)EPackage.Registry.INSTANCE.getEPackage(Requirements_textPackage.eNS_URI);

		// Create type parameters

		// Set bounds for type parameters

		// Add supertypes to classes
		logicalTransformationModuleEClass.getESuperTypes().add(theModule_managementPackage.getModule());
		taskTagEClass.getESuperTypes().add(theRequirements_textPackage.getTag());

		// Initialize classes, features, and operations; add parameters
		initEClass(logicalTransformationModuleEClass, LogicalTransformationModule.class, "LogicalTransformationModule", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getLogicalTransformationModule_TaskTags(), this.getTaskTag(), null, "taskTags", null, 0, -1, LogicalTransformationModule.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getLogicalTransformationModule_ScenarioTags(), theScenariosPackage.getScenarioTag(), null, "scenarioTags", null, 0, -1, LogicalTransformationModule.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getLogicalTransformationModule_ScenarioSetTags(), theScenariosPackage.getScenarioSetTag(), null, "scenarioSetTags", null, 0, -1, LogicalTransformationModule.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getLogicalTransformationModule_SubProcesses(), theBpmn_litePackage.getSubProcess(), null, "subProcesses", null, 0, -1, LogicalTransformationModule.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(taskTagEClass, TaskTag.class, "TaskTag", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);

		// Create resource
		createResource(eNS_URI);
	}

} //Logical_transformationsPackageImpl
