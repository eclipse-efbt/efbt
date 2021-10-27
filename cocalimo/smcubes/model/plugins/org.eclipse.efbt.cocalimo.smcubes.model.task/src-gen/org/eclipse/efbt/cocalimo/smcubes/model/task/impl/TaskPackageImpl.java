/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.task.impl;

import org.eclipse.efbt.cocalimo.core.model.module_management.Module_managementPackage;

import org.eclipse.efbt.cocalimo.core.model.requirements_text.Requirements_textPackage;

import org.eclipse.efbt.cocalimo.smcubes.model.core.CorePackage;
import org.eclipse.efbt.cocalimo.smcubes.model.data_definition.Data_definitionPackage;
import org.eclipse.efbt.cocalimo.smcubes.model.scenarios.ScenariosPackage;

import org.eclipse.efbt.cocalimo.smcubes.model.scenarios.impl.ScenariosPackageImpl;

import org.eclipse.efbt.cocalimo.smcubes.model.task.DataProcessingTask;
import org.eclipse.efbt.cocalimo.smcubes.model.task.LeafTask;
import org.eclipse.efbt.cocalimo.smcubes.model.task.ParallelChoiceTask;
import org.eclipse.efbt.cocalimo.smcubes.model.task.ParallelTask;
import org.eclipse.efbt.cocalimo.smcubes.model.task.ReadDataSourceTask;
import org.eclipse.efbt.cocalimo.smcubes.model.task.ReportCellCreationTask;
import org.eclipse.efbt.cocalimo.smcubes.model.task.SerialTask;
import org.eclipse.efbt.cocalimo.smcubes.model.task.Task;
import org.eclipse.efbt.cocalimo.smcubes.model.task.TaskFactory;
import org.eclipse.efbt.cocalimo.smcubes.model.task.TaskModule;
import org.eclipse.efbt.cocalimo.smcubes.model.task.TaskPackage;
import org.eclipse.efbt.cocalimo.smcubes.model.task.TaskSet;
import org.eclipse.efbt.cocalimo.smcubes.model.task.TaskTag;

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
public class TaskPackageImpl extends EPackageImpl implements TaskPackage {
	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass dataProcessingTaskEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass leafTaskEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass parallelTaskEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass serialTaskEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass taskSetEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass readDataSourceTaskEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass reportCellCreationTaskEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass taskEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass taskModuleEClass = null;

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
	private EClass parallelChoiceTaskEClass = null;

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
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.task.TaskPackage#eNS_URI
	 * @see #init()
	 * @generated
	 */
	private TaskPackageImpl() {
		super(eNS_URI, TaskFactory.eINSTANCE);
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
	 * <p>This method is used to initialize {@link TaskPackage#eINSTANCE} when that field is accessed.
	 * Clients should not invoke it directly. Instead, they should simply access that field to obtain the package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #eNS_URI
	 * @see #createPackageContents()
	 * @see #initializePackageContents()
	 * @generated
	 */
	public static TaskPackage init() {
		if (isInited) return (TaskPackage)EPackage.Registry.INSTANCE.getEPackage(TaskPackage.eNS_URI);

		// Obtain or create and register package
		Object registeredTaskPackage = EPackage.Registry.INSTANCE.get(eNS_URI);
		TaskPackageImpl theTaskPackage = registeredTaskPackage instanceof TaskPackageImpl ? (TaskPackageImpl)registeredTaskPackage : new TaskPackageImpl();

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

		// Create package meta-data objects
		theTaskPackage.createPackageContents();
		theScenariosPackage.createPackageContents();

		// Initialize created meta-data
		theTaskPackage.initializePackageContents();
		theScenariosPackage.initializePackageContents();

		// Mark meta-data to indicate it can't be changed
		theTaskPackage.freeze();

		// Update the registry and return the package
		EPackage.Registry.INSTANCE.put(TaskPackage.eNS_URI, theTaskPackage);
		return theTaskPackage;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getDataProcessingTask() {
		return dataProcessingTaskEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getDataProcessingTask_TargetAttribute() {
		return (EReference)dataProcessingTaskEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getDataProcessingTask_RequiredAtrributes() {
		return (EReference)dataProcessingTaskEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getLeafTask() {
		return leafTaskEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getParallelTask() {
		return parallelTaskEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getSerialTask() {
		return serialTaskEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getTaskSet() {
		return taskSetEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getTaskSet_SubTasks() {
		return (EReference)taskSetEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getReadDataSourceTask() {
		return readDataSourceTaskEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getReadDataSourceTask_Entity() {
		return (EReference)readDataSourceTaskEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getReportCellCreationTask() {
		return reportCellCreationTaskEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getTask() {
		return taskEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getTask_Name() {
		return (EAttribute)taskEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getTask_Scenario() {
		return (EReference)taskEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getTask_ScenarioSet() {
		return (EReference)taskEClass.getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getTask_UsedInSubset() {
		return (EAttribute)taskEClass.getEStructuralFeatures().get(3);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getTaskModule() {
		return taskModuleEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getTaskModule_Tasks() {
		return (EReference)taskModuleEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getTaskModule_TaskTags() {
		return (EReference)taskModuleEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getTaskModule_SceanrioTags() {
		return (EReference)taskModuleEClass.getEStructuralFeatures().get(2);
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
	public EReference getTaskTag_Task() {
		return (EReference)taskTagEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getParallelChoiceTask() {
		return parallelChoiceTaskEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public TaskFactory getTaskFactory() {
		return (TaskFactory)getEFactoryInstance();
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
		dataProcessingTaskEClass = createEClass(DATA_PROCESSING_TASK);
		createEReference(dataProcessingTaskEClass, DATA_PROCESSING_TASK__TARGET_ATTRIBUTE);
		createEReference(dataProcessingTaskEClass, DATA_PROCESSING_TASK__REQUIRED_ATRRIBUTES);

		leafTaskEClass = createEClass(LEAF_TASK);

		parallelTaskEClass = createEClass(PARALLEL_TASK);

		serialTaskEClass = createEClass(SERIAL_TASK);

		taskSetEClass = createEClass(TASK_SET);
		createEReference(taskSetEClass, TASK_SET__SUB_TASKS);

		readDataSourceTaskEClass = createEClass(READ_DATA_SOURCE_TASK);
		createEReference(readDataSourceTaskEClass, READ_DATA_SOURCE_TASK__ENTITY);

		reportCellCreationTaskEClass = createEClass(REPORT_CELL_CREATION_TASK);

		taskEClass = createEClass(TASK);
		createEAttribute(taskEClass, TASK__NAME);
		createEReference(taskEClass, TASK__SCENARIO);
		createEReference(taskEClass, TASK__SCENARIO_SET);
		createEAttribute(taskEClass, TASK__USED_IN_SUBSET);

		taskModuleEClass = createEClass(TASK_MODULE);
		createEReference(taskModuleEClass, TASK_MODULE__TASKS);
		createEReference(taskModuleEClass, TASK_MODULE__TASK_TAGS);
		createEReference(taskModuleEClass, TASK_MODULE__SCEANRIO_TAGS);

		taskTagEClass = createEClass(TASK_TAG);
		createEReference(taskTagEClass, TASK_TAG__TASK);

		parallelChoiceTaskEClass = createEClass(PARALLEL_CHOICE_TASK);
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
		Data_definitionPackage theData_definitionPackage = (Data_definitionPackage)EPackage.Registry.INSTANCE.getEPackage(Data_definitionPackage.eNS_URI);
		EcorePackage theEcorePackage = (EcorePackage)EPackage.Registry.INSTANCE.getEPackage(EcorePackage.eNS_URI);
		ScenariosPackage theScenariosPackage = (ScenariosPackage)EPackage.Registry.INSTANCE.getEPackage(ScenariosPackage.eNS_URI);
		Module_managementPackage theModule_managementPackage = (Module_managementPackage)EPackage.Registry.INSTANCE.getEPackage(Module_managementPackage.eNS_URI);
		Requirements_textPackage theRequirements_textPackage = (Requirements_textPackage)EPackage.Registry.INSTANCE.getEPackage(Requirements_textPackage.eNS_URI);

		// Create type parameters

		// Set bounds for type parameters

		// Add supertypes to classes
		dataProcessingTaskEClass.getESuperTypes().add(this.getLeafTask());
		leafTaskEClass.getESuperTypes().add(this.getTask());
		parallelTaskEClass.getESuperTypes().add(this.getTaskSet());
		serialTaskEClass.getESuperTypes().add(this.getTaskSet());
		taskSetEClass.getESuperTypes().add(this.getTask());
		readDataSourceTaskEClass.getESuperTypes().add(this.getLeafTask());
		reportCellCreationTaskEClass.getESuperTypes().add(this.getLeafTask());
		taskModuleEClass.getESuperTypes().add(theModule_managementPackage.getModule());
		taskTagEClass.getESuperTypes().add(theRequirements_textPackage.getTag());
		parallelChoiceTaskEClass.getESuperTypes().add(this.getTaskSet());

		// Initialize classes, features, and operations; add parameters
		initEClass(dataProcessingTaskEClass, DataProcessingTask.class, "DataProcessingTask", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getDataProcessingTask_TargetAttribute(), theData_definitionPackage.getCUBE_STRUCTURE_ITEM(), null, "targetAttribute", null, 0, 1, DataProcessingTask.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getDataProcessingTask_RequiredAtrributes(), theData_definitionPackage.getCUBE_STRUCTURE_ITEM(), null, "requiredAtrributes", null, 0, -1, DataProcessingTask.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(leafTaskEClass, LeafTask.class, "LeafTask", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);

		initEClass(parallelTaskEClass, ParallelTask.class, "ParallelTask", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);

		initEClass(serialTaskEClass, SerialTask.class, "SerialTask", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);

		initEClass(taskSetEClass, TaskSet.class, "TaskSet", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getTaskSet_SubTasks(), this.getTask(), null, "subTasks", null, 0, -1, TaskSet.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(readDataSourceTaskEClass, ReadDataSourceTask.class, "ReadDataSourceTask", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getReadDataSourceTask_Entity(), theEcorePackage.getEClass(), null, "entity", null, 0, 1, ReadDataSourceTask.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(reportCellCreationTaskEClass, ReportCellCreationTask.class, "ReportCellCreationTask", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);

		initEClass(taskEClass, Task.class, "Task", IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEAttribute(getTask_Name(), ecorePackage.getEString(), "name", null, 0, 1, Task.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getTask_Scenario(), theScenariosPackage.getScenario(), null, "scenario", null, 0, 1, Task.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getTask_ScenarioSet(), theScenariosPackage.getScenarioSet(), null, "scenarioSet", null, 0, 1, Task.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getTask_UsedInSubset(), ecorePackage.getEBoolean(), "usedInSubset", null, 0, 1, Task.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(taskModuleEClass, TaskModule.class, "TaskModule", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getTaskModule_Tasks(), this.getTask(), null, "Tasks", null, 0, -1, TaskModule.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getTaskModule_TaskTags(), this.getTaskTag(), null, "taskTags", null, 0, -1, TaskModule.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getTaskModule_SceanrioTags(), theScenariosPackage.getScenarioTag(), null, "SceanrioTags", null, 0, -1, TaskModule.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(taskTagEClass, TaskTag.class, "TaskTag", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getTaskTag_Task(), this.getTask(), null, "Task", null, 0, 1, TaskTag.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(parallelChoiceTaskEClass, ParallelChoiceTask.class, "ParallelChoiceTask", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);

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
			   "license", "Copyright (c) 2019 Bird Software Solutions Ltd\n All rights reserved. This file and the accompanying materials are made available under the terms of the Eclipse Public License v2.0 which accompanies this distribution, and is available at http://www.eclipse.org/legal/epl-v20.html\n\nSPDX-License-Identifier: EPL-2.0 "
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
		  (leafTaskEClass,
		   source,
		   new String[] {
			   "name", "LeafTask",
			   "kind", "elementOnly"
		   });
		addAnnotation
		  (parallelTaskEClass,
		   source,
		   new String[] {
			   "name", "ParallelTask",
			   "kind", "elementOnly"
		   });
		addAnnotation
		  (serialTaskEClass,
		   source,
		   new String[] {
			   "name", "SerialTask",
			   "kind", "elementOnly"
		   });
		addAnnotation
		  (taskSetEClass,
		   source,
		   new String[] {
			   "name", "TaskSet",
			   "kind", "elementOnly"
		   });
		addAnnotation
		  (getTaskSet_SubTasks(),
		   source,
		   new String[] {
			   "kind", "element",
			   "name", "subTasks"
		   });
		addAnnotation
		  (readDataSourceTaskEClass,
		   source,
		   new String[] {
			   "name", "ReadDataSourceTask",
			   "kind", "elementOnly"
		   });
		addAnnotation
		  (reportCellCreationTaskEClass,
		   source,
		   new String[] {
			   "name", "ReportCellCreationTask",
			   "kind", "elementOnly"
		   });
		addAnnotation
		  (taskEClass,
		   source,
		   new String[] {
			   "name", "Task",
			   "kind", "empty"
		   });
		addAnnotation
		  (getTask_Name(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "name"
		   });
		addAnnotation
		  (getTask_ScenarioSet(),
		   source,
		   new String[] {
			   "kind", "element",
			   "name", "scenarioSet"
		   });
		addAnnotation
		  (taskModuleEClass,
		   source,
		   new String[] {
			   "name", "TaskModule",
			   "kind", "elementOnly"
		   });
		addAnnotation
		  (getTaskModule_Tasks(),
		   source,
		   new String[] {
			   "kind", "element",
			   "name", "Tasks"
		   });
		addAnnotation
		  (parallelChoiceTaskEClass,
		   source,
		   new String[] {
			   "name", "ParallelChoiceTask",
			   "kind", "elementOnly"
		   });
	}

} //TaskPackageImpl
