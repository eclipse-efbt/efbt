/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.task.util;

import org.eclipse.efbt.cocalimo.core.model.requirements_text.Tag;

import org.eclipse.efbt.cocalimo.smcubes.model.task.*;

import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.EPackage;

import org.eclipse.emf.ecore.util.Switch;

/**
 * <!-- begin-user-doc -->
 * The <b>Switch</b> for the model's inheritance hierarchy.
 * It supports the call {@link #doSwitch(EObject) doSwitch(object)}
 * to invoke the <code>caseXXX</code> method for each class of the model,
 * starting with the actual class of the object
 * and proceeding up the inheritance hierarchy
 * until a non-null result is returned,
 * which is the result of the switch.
 * <!-- end-user-doc -->
 * @see org.eclipse.efbt.cocalimo.smcubes.model.task.TaskPackage
 * @generated
 */
public class TaskSwitch<T> extends Switch<T> {
	/**
	 * The cached model package
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected static TaskPackage modelPackage;

	/**
	 * Creates an instance of the switch.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public TaskSwitch() {
		if (modelPackage == null) {
			modelPackage = TaskPackage.eINSTANCE;
		}
	}

	/**
	 * Checks whether this is a switch for the given package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param ePackage the package in question.
	 * @return whether this is a switch for the given package.
	 * @generated
	 */
	@Override
	protected boolean isSwitchFor(EPackage ePackage) {
		return ePackage == modelPackage;
	}

	/**
	 * Calls <code>caseXXX</code> for each class of the model until one returns a non null result; it yields that result.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the first non-null result returned by a <code>caseXXX</code> call.
	 * @generated
	 */
	@Override
	protected T doSwitch(int classifierID, EObject theEObject) {
		switch (classifierID) {
			case TaskPackage.DATA_PROCESSING_TASK: {
				DataProcessingTask dataProcessingTask = (DataProcessingTask)theEObject;
				T result = caseDataProcessingTask(dataProcessingTask);
				if (result == null) result = caseLeafTask(dataProcessingTask);
				if (result == null) result = caseTask(dataProcessingTask);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case TaskPackage.LEAF_TASK: {
				LeafTask leafTask = (LeafTask)theEObject;
				T result = caseLeafTask(leafTask);
				if (result == null) result = caseTask(leafTask);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case TaskPackage.PARALLEL_TASK: {
				ParallelTask parallelTask = (ParallelTask)theEObject;
				T result = caseParallelTask(parallelTask);
				if (result == null) result = caseTaskSet(parallelTask);
				if (result == null) result = caseTask(parallelTask);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case TaskPackage.SERIAL_TASK: {
				SerialTask serialTask = (SerialTask)theEObject;
				T result = caseSerialTask(serialTask);
				if (result == null) result = caseTaskSet(serialTask);
				if (result == null) result = caseTask(serialTask);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case TaskPackage.TASK_SET: {
				TaskSet taskSet = (TaskSet)theEObject;
				T result = caseTaskSet(taskSet);
				if (result == null) result = caseTask(taskSet);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case TaskPackage.READ_DATA_SOURCE_TASK: {
				ReadDataSourceTask readDataSourceTask = (ReadDataSourceTask)theEObject;
				T result = caseReadDataSourceTask(readDataSourceTask);
				if (result == null) result = caseLeafTask(readDataSourceTask);
				if (result == null) result = caseTask(readDataSourceTask);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case TaskPackage.REPORT_CELL_CREATION_TASK: {
				ReportCellCreationTask reportCellCreationTask = (ReportCellCreationTask)theEObject;
				T result = caseReportCellCreationTask(reportCellCreationTask);
				if (result == null) result = caseLeafTask(reportCellCreationTask);
				if (result == null) result = caseTask(reportCellCreationTask);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case TaskPackage.TASK: {
				Task task = (Task)theEObject;
				T result = caseTask(task);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case TaskPackage.TASK_MODULE: {
				TaskModule taskModule = (TaskModule)theEObject;
				T result = caseTaskModule(taskModule);
				if (result == null) result = caseModule(taskModule);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case TaskPackage.TASK_TAG: {
				TaskTag taskTag = (TaskTag)theEObject;
				T result = caseTaskTag(taskTag);
				if (result == null) result = caseTag(taskTag);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case TaskPackage.PARALLEL_CHOICE_TASK: {
				ParallelChoiceTask parallelChoiceTask = (ParallelChoiceTask)theEObject;
				T result = caseParallelChoiceTask(parallelChoiceTask);
				if (result == null) result = caseTaskSet(parallelChoiceTask);
				if (result == null) result = caseTask(parallelChoiceTask);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			default: return defaultCase(theEObject);
		}
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Data Processing Task</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Data Processing Task</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseDataProcessingTask(DataProcessingTask object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Leaf Task</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Leaf Task</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseLeafTask(LeafTask object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Parallel Task</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Parallel Task</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseParallelTask(ParallelTask object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Serial Task</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Serial Task</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseSerialTask(SerialTask object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Set</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Set</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseTaskSet(TaskSet object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Read Data Source Task</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Read Data Source Task</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseReadDataSourceTask(ReadDataSourceTask object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Report Cell Creation Task</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Report Cell Creation Task</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseReportCellCreationTask(ReportCellCreationTask object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Task</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Task</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseTask(Task object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Module</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Module</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseTaskModule(TaskModule object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Tag</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Tag</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseTaskTag(TaskTag object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Parallel Choice Task</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Parallel Choice Task</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseParallelChoiceTask(ParallelChoiceTask object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Module</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Module</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseModule(org.eclipse.efbt.cocalimo.core.model.module_management.Module object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Tag</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Tag</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseTag(Tag object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>EObject</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch, but this is the last case anyway.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>EObject</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject)
	 * @generated
	 */
	@Override
	public T defaultCase(EObject object) {
		return null;
	}

} //TaskSwitch
