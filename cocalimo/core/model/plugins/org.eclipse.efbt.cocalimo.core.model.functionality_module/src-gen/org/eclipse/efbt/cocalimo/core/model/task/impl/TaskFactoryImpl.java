/**
 */
package org.eclipse.efbt.cocalimo.core.model.task.impl;

import org.eclipse.efbt.cocalimo.core.model.task.*;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.EPackage;

import org.eclipse.emf.ecore.impl.EFactoryImpl;

import org.eclipse.emf.ecore.plugin.EcorePlugin;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model <b>Factory</b>.
 * <!-- end-user-doc -->
 * @generated
 */
public class TaskFactoryImpl extends EFactoryImpl implements TaskFactory {
	/**
	 * Creates the default factory implementation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public static TaskFactory init() {
		try {
			TaskFactory theTaskFactory = (TaskFactory)EPackage.Registry.INSTANCE.getEFactory(TaskPackage.eNS_URI);
			if (theTaskFactory != null) {
				return theTaskFactory;
			}
		}
		catch (Exception exception) {
			EcorePlugin.INSTANCE.log(exception);
		}
		return new TaskFactoryImpl();
	}

	/**
	 * Creates an instance of the factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public TaskFactoryImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EObject create(EClass eClass) {
		switch (eClass.getClassifierID()) {
			case TaskPackage.DATA_PROCESSING_TASK: return createDataProcessingTask();
			case TaskPackage.LEAF_TASK: return createLeafTask();
			case TaskPackage.PARALLEL_TASK: return createParallelTask();
			case TaskPackage.SERIAL_TASK: return createSerialTask();
			case TaskPackage.TASK_SET: return createTaskSet();
			case TaskPackage.READ_DATA_SOURCE_TASK: return createReadDataSourceTask();
			case TaskPackage.REPORT_CELL_CREATION_TASK: return createReportCellCreationTask();
			case TaskPackage.TASK_MODULE: return createTaskModule();
			case TaskPackage.TASK_TAG: return createTaskTag();
			case TaskPackage.PARALLEL_CHOICE_TASK: return createParallelChoiceTask();
			default:
				throw new IllegalArgumentException("The class '" + eClass.getName() + "' is not a valid classifier");
		}
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public DataProcessingTask createDataProcessingTask() {
		DataProcessingTaskImpl dataProcessingTask = new DataProcessingTaskImpl();
		return dataProcessingTask;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public LeafTask createLeafTask() {
		LeafTaskImpl leafTask = new LeafTaskImpl();
		return leafTask;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public ParallelTask createParallelTask() {
		ParallelTaskImpl parallelTask = new ParallelTaskImpl();
		return parallelTask;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public SerialTask createSerialTask() {
		SerialTaskImpl serialTask = new SerialTaskImpl();
		return serialTask;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public TaskSet createTaskSet() {
		TaskSetImpl taskSet = new TaskSetImpl();
		return taskSet;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public ReadDataSourceTask createReadDataSourceTask() {
		ReadDataSourceTaskImpl readDataSourceTask = new ReadDataSourceTaskImpl();
		return readDataSourceTask;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public ReportCellCreationTask createReportCellCreationTask() {
		ReportCellCreationTaskImpl reportCellCreationTask = new ReportCellCreationTaskImpl();
		return reportCellCreationTask;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public TaskModule createTaskModule() {
		TaskModuleImpl taskModule = new TaskModuleImpl();
		return taskModule;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public TaskTag createTaskTag() {
		TaskTagImpl taskTag = new TaskTagImpl();
		return taskTag;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public ParallelChoiceTask createParallelChoiceTask() {
		ParallelChoiceTaskImpl parallelChoiceTask = new ParallelChoiceTaskImpl();
		return parallelChoiceTask;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public TaskPackage getTaskPackage() {
		return (TaskPackage)getEPackage();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @deprecated
	 * @generated
	 */
	@Deprecated
	public static TaskPackage getPackage() {
		return TaskPackage.eINSTANCE;
	}

} //TaskFactoryImpl
