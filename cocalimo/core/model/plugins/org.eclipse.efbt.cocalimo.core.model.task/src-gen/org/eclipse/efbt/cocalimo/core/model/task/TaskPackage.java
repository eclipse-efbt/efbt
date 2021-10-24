/**
 */
package org.eclipse.efbt.cocalimo.core.model.task;

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
 * The task package describes Tasks which are sued to describe the functional breakdown and sceanrios of behaviour understood by a domain expert.
 * <!-- end-model-doc -->
 * @see org.eclipse.efbt.cocalimo.core.model.task.TaskFactory
 * @model kind="package"
 *        annotation="license license='Copyright (c) 2019 Bird Software Solutions Ltd\n All rights reserved. This file and the accompanying materials are made available under the terms of the Eclipse Public License v2.0 which accompanies this distribution, and is available at http://www.eclipse.org/legal/epl-v20.html\n\nSPDX-License-Identifier: EPL-2.0 '"
 * @generated
 */
public interface TaskPackage extends EPackage {
	/**
	 * The package name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNAME = "task";

	/**
	 * The package namespace URI.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNS_URI = "http://www.eclipse.org/efbt/task";

	/**
	 * The package namespace name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNS_PREFIX = "task";

	/**
	 * The singleton instance of the package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	TaskPackage eINSTANCE = org.eclipse.efbt.cocalimo.core.model.task.impl.TaskPackageImpl.init();

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.core.model.task.impl.TaskImpl <em>Task</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.core.model.task.impl.TaskImpl
	 * @see org.eclipse.efbt.cocalimo.core.model.task.impl.TaskPackageImpl#getTask()
	 * @generated
	 */
	int TASK = 7;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TASK__NAME = 0;

	/**
	 * The feature id for the '<em><b>Scenario</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TASK__SCENARIO = 1;

	/**
	 * The feature id for the '<em><b>Scenario Set</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TASK__SCENARIO_SET = 2;

	/**
	 * The feature id for the '<em><b>Used In Subset</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TASK__USED_IN_SUBSET = 3;

	/**
	 * The number of structural features of the '<em>Task</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TASK_FEATURE_COUNT = 4;

	/**
	 * The number of operations of the '<em>Task</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TASK_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.core.model.task.impl.LeafTaskImpl <em>Leaf Task</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.core.model.task.impl.LeafTaskImpl
	 * @see org.eclipse.efbt.cocalimo.core.model.task.impl.TaskPackageImpl#getLeafTask()
	 * @generated
	 */
	int LEAF_TASK = 1;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int LEAF_TASK__NAME = TASK__NAME;

	/**
	 * The feature id for the '<em><b>Scenario</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int LEAF_TASK__SCENARIO = TASK__SCENARIO;

	/**
	 * The feature id for the '<em><b>Scenario Set</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int LEAF_TASK__SCENARIO_SET = TASK__SCENARIO_SET;

	/**
	 * The feature id for the '<em><b>Used In Subset</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int LEAF_TASK__USED_IN_SUBSET = TASK__USED_IN_SUBSET;

	/**
	 * The number of structural features of the '<em>Leaf Task</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int LEAF_TASK_FEATURE_COUNT = TASK_FEATURE_COUNT + 0;

	/**
	 * The number of operations of the '<em>Leaf Task</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int LEAF_TASK_OPERATION_COUNT = TASK_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.core.model.task.impl.DataProcessingTaskImpl <em>Data Processing Task</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.core.model.task.impl.DataProcessingTaskImpl
	 * @see org.eclipse.efbt.cocalimo.core.model.task.impl.TaskPackageImpl#getDataProcessingTask()
	 * @generated
	 */
	int DATA_PROCESSING_TASK = 0;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DATA_PROCESSING_TASK__NAME = LEAF_TASK__NAME;

	/**
	 * The feature id for the '<em><b>Scenario</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DATA_PROCESSING_TASK__SCENARIO = LEAF_TASK__SCENARIO;

	/**
	 * The feature id for the '<em><b>Scenario Set</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DATA_PROCESSING_TASK__SCENARIO_SET = LEAF_TASK__SCENARIO_SET;

	/**
	 * The feature id for the '<em><b>Used In Subset</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DATA_PROCESSING_TASK__USED_IN_SUBSET = LEAF_TASK__USED_IN_SUBSET;

	/**
	 * The number of structural features of the '<em>Data Processing Task</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DATA_PROCESSING_TASK_FEATURE_COUNT = LEAF_TASK_FEATURE_COUNT + 0;

	/**
	 * The number of operations of the '<em>Data Processing Task</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DATA_PROCESSING_TASK_OPERATION_COUNT = LEAF_TASK_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.core.model.task.impl.TaskSetImpl <em>Set</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.core.model.task.impl.TaskSetImpl
	 * @see org.eclipse.efbt.cocalimo.core.model.task.impl.TaskPackageImpl#getTaskSet()
	 * @generated
	 */
	int TASK_SET = 4;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TASK_SET__NAME = TASK__NAME;

	/**
	 * The feature id for the '<em><b>Scenario</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TASK_SET__SCENARIO = TASK__SCENARIO;

	/**
	 * The feature id for the '<em><b>Scenario Set</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TASK_SET__SCENARIO_SET = TASK__SCENARIO_SET;

	/**
	 * The feature id for the '<em><b>Used In Subset</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TASK_SET__USED_IN_SUBSET = TASK__USED_IN_SUBSET;

	/**
	 * The feature id for the '<em><b>Sub Tasks</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TASK_SET__SUB_TASKS = TASK_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Set</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TASK_SET_FEATURE_COUNT = TASK_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Set</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TASK_SET_OPERATION_COUNT = TASK_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.core.model.task.impl.ParallelTaskImpl <em>Parallel Task</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.core.model.task.impl.ParallelTaskImpl
	 * @see org.eclipse.efbt.cocalimo.core.model.task.impl.TaskPackageImpl#getParallelTask()
	 * @generated
	 */
	int PARALLEL_TASK = 2;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PARALLEL_TASK__NAME = TASK_SET__NAME;

	/**
	 * The feature id for the '<em><b>Scenario</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PARALLEL_TASK__SCENARIO = TASK_SET__SCENARIO;

	/**
	 * The feature id for the '<em><b>Scenario Set</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PARALLEL_TASK__SCENARIO_SET = TASK_SET__SCENARIO_SET;

	/**
	 * The feature id for the '<em><b>Used In Subset</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PARALLEL_TASK__USED_IN_SUBSET = TASK_SET__USED_IN_SUBSET;

	/**
	 * The feature id for the '<em><b>Sub Tasks</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PARALLEL_TASK__SUB_TASKS = TASK_SET__SUB_TASKS;

	/**
	 * The number of structural features of the '<em>Parallel Task</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PARALLEL_TASK_FEATURE_COUNT = TASK_SET_FEATURE_COUNT + 0;

	/**
	 * The number of operations of the '<em>Parallel Task</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PARALLEL_TASK_OPERATION_COUNT = TASK_SET_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.core.model.task.impl.SerialTaskImpl <em>Serial Task</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.core.model.task.impl.SerialTaskImpl
	 * @see org.eclipse.efbt.cocalimo.core.model.task.impl.TaskPackageImpl#getSerialTask()
	 * @generated
	 */
	int SERIAL_TASK = 3;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SERIAL_TASK__NAME = TASK_SET__NAME;

	/**
	 * The feature id for the '<em><b>Scenario</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SERIAL_TASK__SCENARIO = TASK_SET__SCENARIO;

	/**
	 * The feature id for the '<em><b>Scenario Set</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SERIAL_TASK__SCENARIO_SET = TASK_SET__SCENARIO_SET;

	/**
	 * The feature id for the '<em><b>Used In Subset</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SERIAL_TASK__USED_IN_SUBSET = TASK_SET__USED_IN_SUBSET;

	/**
	 * The feature id for the '<em><b>Sub Tasks</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SERIAL_TASK__SUB_TASKS = TASK_SET__SUB_TASKS;

	/**
	 * The number of structural features of the '<em>Serial Task</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SERIAL_TASK_FEATURE_COUNT = TASK_SET_FEATURE_COUNT + 0;

	/**
	 * The number of operations of the '<em>Serial Task</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SERIAL_TASK_OPERATION_COUNT = TASK_SET_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.core.model.task.impl.ReadDataSourceTaskImpl <em>Read Data Source Task</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.core.model.task.impl.ReadDataSourceTaskImpl
	 * @see org.eclipse.efbt.cocalimo.core.model.task.impl.TaskPackageImpl#getReadDataSourceTask()
	 * @generated
	 */
	int READ_DATA_SOURCE_TASK = 5;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int READ_DATA_SOURCE_TASK__NAME = LEAF_TASK__NAME;

	/**
	 * The feature id for the '<em><b>Scenario</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int READ_DATA_SOURCE_TASK__SCENARIO = LEAF_TASK__SCENARIO;

	/**
	 * The feature id for the '<em><b>Scenario Set</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int READ_DATA_SOURCE_TASK__SCENARIO_SET = LEAF_TASK__SCENARIO_SET;

	/**
	 * The feature id for the '<em><b>Used In Subset</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int READ_DATA_SOURCE_TASK__USED_IN_SUBSET = LEAF_TASK__USED_IN_SUBSET;

	/**
	 * The feature id for the '<em><b>Entity</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int READ_DATA_SOURCE_TASK__ENTITY = LEAF_TASK_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Read Data Source Task</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int READ_DATA_SOURCE_TASK_FEATURE_COUNT = LEAF_TASK_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Read Data Source Task</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int READ_DATA_SOURCE_TASK_OPERATION_COUNT = LEAF_TASK_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.core.model.task.impl.ReportCellCreationTaskImpl <em>Report Cell Creation Task</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.core.model.task.impl.ReportCellCreationTaskImpl
	 * @see org.eclipse.efbt.cocalimo.core.model.task.impl.TaskPackageImpl#getReportCellCreationTask()
	 * @generated
	 */
	int REPORT_CELL_CREATION_TASK = 6;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REPORT_CELL_CREATION_TASK__NAME = LEAF_TASK__NAME;

	/**
	 * The feature id for the '<em><b>Scenario</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REPORT_CELL_CREATION_TASK__SCENARIO = LEAF_TASK__SCENARIO;

	/**
	 * The feature id for the '<em><b>Scenario Set</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REPORT_CELL_CREATION_TASK__SCENARIO_SET = LEAF_TASK__SCENARIO_SET;

	/**
	 * The feature id for the '<em><b>Used In Subset</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REPORT_CELL_CREATION_TASK__USED_IN_SUBSET = LEAF_TASK__USED_IN_SUBSET;

	/**
	 * The number of structural features of the '<em>Report Cell Creation Task</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REPORT_CELL_CREATION_TASK_FEATURE_COUNT = LEAF_TASK_FEATURE_COUNT + 0;

	/**
	 * The number of operations of the '<em>Report Cell Creation Task</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REPORT_CELL_CREATION_TASK_OPERATION_COUNT = LEAF_TASK_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.core.model.task.impl.TaskModuleImpl <em>Module</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.core.model.task.impl.TaskModuleImpl
	 * @see org.eclipse.efbt.cocalimo.core.model.task.impl.TaskPackageImpl#getTaskModule()
	 * @generated
	 */
	int TASK_MODULE = 8;

	/**
	 * The feature id for the '<em><b>Dependencies</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TASK_MODULE__DEPENDENCIES = Module_managementPackage.MODULE__DEPENDENCIES;

	/**
	 * The feature id for the '<em><b>The Description</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TASK_MODULE__THE_DESCRIPTION = Module_managementPackage.MODULE__THE_DESCRIPTION;

	/**
	 * The feature id for the '<em><b>License</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TASK_MODULE__LICENSE = Module_managementPackage.MODULE__LICENSE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TASK_MODULE__NAME = Module_managementPackage.MODULE__NAME;

	/**
	 * The feature id for the '<em><b>Version</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TASK_MODULE__VERSION = Module_managementPackage.MODULE__VERSION;

	/**
	 * The feature id for the '<em><b>Long Name</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TASK_MODULE__LONG_NAME = Module_managementPackage.MODULE__LONG_NAME;

	/**
	 * The feature id for the '<em><b>Tasks</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TASK_MODULE__TASKS = Module_managementPackage.MODULE_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Task Tags</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TASK_MODULE__TASK_TAGS = Module_managementPackage.MODULE_FEATURE_COUNT + 1;

	/**
	 * The feature id for the '<em><b>Sceanrio Tags</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TASK_MODULE__SCEANRIO_TAGS = Module_managementPackage.MODULE_FEATURE_COUNT + 2;

	/**
	 * The number of structural features of the '<em>Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TASK_MODULE_FEATURE_COUNT = Module_managementPackage.MODULE_FEATURE_COUNT + 3;

	/**
	 * The number of operations of the '<em>Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TASK_MODULE_OPERATION_COUNT = Module_managementPackage.MODULE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.core.model.task.impl.TaskTagImpl <em>Tag</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.core.model.task.impl.TaskTagImpl
	 * @see org.eclipse.efbt.cocalimo.core.model.task.impl.TaskPackageImpl#getTaskTag()
	 * @generated
	 */
	int TASK_TAG = 9;

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
	 * The number of structural features of the '<em>Tag</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TASK_TAG_FEATURE_COUNT = Requirements_textPackage.TAG_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Tag</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TASK_TAG_OPERATION_COUNT = Requirements_textPackage.TAG_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.core.model.task.impl.ParallelChoiceTaskImpl <em>Parallel Choice Task</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.core.model.task.impl.ParallelChoiceTaskImpl
	 * @see org.eclipse.efbt.cocalimo.core.model.task.impl.TaskPackageImpl#getParallelChoiceTask()
	 * @generated
	 */
	int PARALLEL_CHOICE_TASK = 10;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PARALLEL_CHOICE_TASK__NAME = TASK_SET__NAME;

	/**
	 * The feature id for the '<em><b>Scenario</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PARALLEL_CHOICE_TASK__SCENARIO = TASK_SET__SCENARIO;

	/**
	 * The feature id for the '<em><b>Scenario Set</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PARALLEL_CHOICE_TASK__SCENARIO_SET = TASK_SET__SCENARIO_SET;

	/**
	 * The feature id for the '<em><b>Used In Subset</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PARALLEL_CHOICE_TASK__USED_IN_SUBSET = TASK_SET__USED_IN_SUBSET;

	/**
	 * The feature id for the '<em><b>Sub Tasks</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PARALLEL_CHOICE_TASK__SUB_TASKS = TASK_SET__SUB_TASKS;

	/**
	 * The number of structural features of the '<em>Parallel Choice Task</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PARALLEL_CHOICE_TASK_FEATURE_COUNT = TASK_SET_FEATURE_COUNT + 0;

	/**
	 * The number of operations of the '<em>Parallel Choice Task</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PARALLEL_CHOICE_TASK_OPERATION_COUNT = TASK_SET_OPERATION_COUNT + 0;


	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.core.model.task.DataProcessingTask <em>Data Processing Task</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Data Processing Task</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.task.DataProcessingTask
	 * @generated
	 */
	EClass getDataProcessingTask();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.core.model.task.LeafTask <em>Leaf Task</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Leaf Task</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.task.LeafTask
	 * @generated
	 */
	EClass getLeafTask();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.core.model.task.ParallelTask <em>Parallel Task</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Parallel Task</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.task.ParallelTask
	 * @generated
	 */
	EClass getParallelTask();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.core.model.task.SerialTask <em>Serial Task</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Serial Task</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.task.SerialTask
	 * @generated
	 */
	EClass getSerialTask();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.core.model.task.TaskSet <em>Set</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Set</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.task.TaskSet
	 * @generated
	 */
	EClass getTaskSet();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.cocalimo.core.model.task.TaskSet#getSubTasks <em>Sub Tasks</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Sub Tasks</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.task.TaskSet#getSubTasks()
	 * @see #getTaskSet()
	 * @generated
	 */
	EReference getTaskSet_SubTasks();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.core.model.task.ReadDataSourceTask <em>Read Data Source Task</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Read Data Source Task</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.task.ReadDataSourceTask
	 * @generated
	 */
	EClass getReadDataSourceTask();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.core.model.task.ReadDataSourceTask#getEntity <em>Entity</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Entity</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.task.ReadDataSourceTask#getEntity()
	 * @see #getReadDataSourceTask()
	 * @generated
	 */
	EReference getReadDataSourceTask_Entity();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.core.model.task.ReportCellCreationTask <em>Report Cell Creation Task</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Report Cell Creation Task</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.task.ReportCellCreationTask
	 * @generated
	 */
	EClass getReportCellCreationTask();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.core.model.task.Task <em>Task</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Task</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.task.Task
	 * @generated
	 */
	EClass getTask();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.core.model.task.Task#getName <em>Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Name</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.task.Task#getName()
	 * @see #getTask()
	 * @generated
	 */
	EAttribute getTask_Name();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.core.model.task.Task#getScenario <em>Scenario</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Scenario</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.task.Task#getScenario()
	 * @see #getTask()
	 * @generated
	 */
	EReference getTask_Scenario();

	/**
	 * Returns the meta object for the containment reference '{@link org.eclipse.efbt.cocalimo.core.model.task.Task#getScenarioSet <em>Scenario Set</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Scenario Set</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.task.Task#getScenarioSet()
	 * @see #getTask()
	 * @generated
	 */
	EReference getTask_ScenarioSet();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.core.model.task.Task#isUsedInSubset <em>Used In Subset</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Used In Subset</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.task.Task#isUsedInSubset()
	 * @see #getTask()
	 * @generated
	 */
	EAttribute getTask_UsedInSubset();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.core.model.task.TaskModule <em>Module</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Module</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.task.TaskModule
	 * @generated
	 */
	EClass getTaskModule();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.cocalimo.core.model.task.TaskModule#getTasks <em>Tasks</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Tasks</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.task.TaskModule#getTasks()
	 * @see #getTaskModule()
	 * @generated
	 */
	EReference getTaskModule_Tasks();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.cocalimo.core.model.task.TaskModule#getTaskTags <em>Task Tags</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Task Tags</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.task.TaskModule#getTaskTags()
	 * @see #getTaskModule()
	 * @generated
	 */
	EReference getTaskModule_TaskTags();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.cocalimo.core.model.task.TaskModule#getSceanrioTags <em>Sceanrio Tags</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Sceanrio Tags</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.task.TaskModule#getSceanrioTags()
	 * @see #getTaskModule()
	 * @generated
	 */
	EReference getTaskModule_SceanrioTags();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.core.model.task.TaskTag <em>Tag</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Tag</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.task.TaskTag
	 * @generated
	 */
	EClass getTaskTag();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.core.model.task.TaskTag#getTask <em>Task</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Task</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.task.TaskTag#getTask()
	 * @see #getTaskTag()
	 * @generated
	 */
	EReference getTaskTag_Task();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.core.model.task.ParallelChoiceTask <em>Parallel Choice Task</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Parallel Choice Task</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.task.ParallelChoiceTask
	 * @generated
	 */
	EClass getParallelChoiceTask();

	/**
	 * Returns the factory that creates the instances of the model.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the factory that creates the instances of the model.
	 * @generated
	 */
	TaskFactory getTaskFactory();

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
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.core.model.task.impl.DataProcessingTaskImpl <em>Data Processing Task</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.core.model.task.impl.DataProcessingTaskImpl
		 * @see org.eclipse.efbt.cocalimo.core.model.task.impl.TaskPackageImpl#getDataProcessingTask()
		 * @generated
		 */
		EClass DATA_PROCESSING_TASK = eINSTANCE.getDataProcessingTask();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.core.model.task.impl.LeafTaskImpl <em>Leaf Task</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.core.model.task.impl.LeafTaskImpl
		 * @see org.eclipse.efbt.cocalimo.core.model.task.impl.TaskPackageImpl#getLeafTask()
		 * @generated
		 */
		EClass LEAF_TASK = eINSTANCE.getLeafTask();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.core.model.task.impl.ParallelTaskImpl <em>Parallel Task</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.core.model.task.impl.ParallelTaskImpl
		 * @see org.eclipse.efbt.cocalimo.core.model.task.impl.TaskPackageImpl#getParallelTask()
		 * @generated
		 */
		EClass PARALLEL_TASK = eINSTANCE.getParallelTask();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.core.model.task.impl.SerialTaskImpl <em>Serial Task</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.core.model.task.impl.SerialTaskImpl
		 * @see org.eclipse.efbt.cocalimo.core.model.task.impl.TaskPackageImpl#getSerialTask()
		 * @generated
		 */
		EClass SERIAL_TASK = eINSTANCE.getSerialTask();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.core.model.task.impl.TaskSetImpl <em>Set</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.core.model.task.impl.TaskSetImpl
		 * @see org.eclipse.efbt.cocalimo.core.model.task.impl.TaskPackageImpl#getTaskSet()
		 * @generated
		 */
		EClass TASK_SET = eINSTANCE.getTaskSet();

		/**
		 * The meta object literal for the '<em><b>Sub Tasks</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference TASK_SET__SUB_TASKS = eINSTANCE.getTaskSet_SubTasks();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.core.model.task.impl.ReadDataSourceTaskImpl <em>Read Data Source Task</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.core.model.task.impl.ReadDataSourceTaskImpl
		 * @see org.eclipse.efbt.cocalimo.core.model.task.impl.TaskPackageImpl#getReadDataSourceTask()
		 * @generated
		 */
		EClass READ_DATA_SOURCE_TASK = eINSTANCE.getReadDataSourceTask();

		/**
		 * The meta object literal for the '<em><b>Entity</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference READ_DATA_SOURCE_TASK__ENTITY = eINSTANCE.getReadDataSourceTask_Entity();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.core.model.task.impl.ReportCellCreationTaskImpl <em>Report Cell Creation Task</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.core.model.task.impl.ReportCellCreationTaskImpl
		 * @see org.eclipse.efbt.cocalimo.core.model.task.impl.TaskPackageImpl#getReportCellCreationTask()
		 * @generated
		 */
		EClass REPORT_CELL_CREATION_TASK = eINSTANCE.getReportCellCreationTask();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.core.model.task.impl.TaskImpl <em>Task</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.core.model.task.impl.TaskImpl
		 * @see org.eclipse.efbt.cocalimo.core.model.task.impl.TaskPackageImpl#getTask()
		 * @generated
		 */
		EClass TASK = eINSTANCE.getTask();

		/**
		 * The meta object literal for the '<em><b>Name</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute TASK__NAME = eINSTANCE.getTask_Name();

		/**
		 * The meta object literal for the '<em><b>Scenario</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference TASK__SCENARIO = eINSTANCE.getTask_Scenario();

		/**
		 * The meta object literal for the '<em><b>Scenario Set</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference TASK__SCENARIO_SET = eINSTANCE.getTask_ScenarioSet();

		/**
		 * The meta object literal for the '<em><b>Used In Subset</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute TASK__USED_IN_SUBSET = eINSTANCE.getTask_UsedInSubset();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.core.model.task.impl.TaskModuleImpl <em>Module</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.core.model.task.impl.TaskModuleImpl
		 * @see org.eclipse.efbt.cocalimo.core.model.task.impl.TaskPackageImpl#getTaskModule()
		 * @generated
		 */
		EClass TASK_MODULE = eINSTANCE.getTaskModule();

		/**
		 * The meta object literal for the '<em><b>Tasks</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference TASK_MODULE__TASKS = eINSTANCE.getTaskModule_Tasks();

		/**
		 * The meta object literal for the '<em><b>Task Tags</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference TASK_MODULE__TASK_TAGS = eINSTANCE.getTaskModule_TaskTags();

		/**
		 * The meta object literal for the '<em><b>Sceanrio Tags</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference TASK_MODULE__SCEANRIO_TAGS = eINSTANCE.getTaskModule_SceanrioTags();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.core.model.task.impl.TaskTagImpl <em>Tag</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.core.model.task.impl.TaskTagImpl
		 * @see org.eclipse.efbt.cocalimo.core.model.task.impl.TaskPackageImpl#getTaskTag()
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
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.core.model.task.impl.ParallelChoiceTaskImpl <em>Parallel Choice Task</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.core.model.task.impl.ParallelChoiceTaskImpl
		 * @see org.eclipse.efbt.cocalimo.core.model.task.impl.TaskPackageImpl#getParallelChoiceTask()
		 * @generated
		 */
		EClass PARALLEL_CHOICE_TASK = eINSTANCE.getParallelChoiceTask();

	}

} //TaskPackage
