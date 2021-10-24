/**
 */
package org.eclipse.efbt.cocalimo.core.model.task;

import org.eclipse.efbt.cocalimo.core.model.scenarios.ScenarioTag;

import org.eclipse.emf.common.util.EList;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Module</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * A module of Tasks
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.task.TaskModule#getTasks <em>Tasks</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.task.TaskModule#getTaskTags <em>Task Tags</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.task.TaskModule#getSceanrioTags <em>Sceanrio Tags</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.core.model.task.TaskPackage#getTaskModule()
 * @model extendedMetaData="name='TaskModule' kind='elementOnly'"
 * @generated
 */
public interface TaskModule extends org.eclipse.efbt.cocalimo.core.model.module_management.Module {
	/**
	 * Returns the value of the '<em><b>Tasks</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.cocalimo.core.model.task.Task}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The  set of Tasks 
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Tasks</em>' containment reference list.
	 * @see org.eclipse.efbt.cocalimo.core.model.task.TaskPackage#getTaskModule_Tasks()
	 * @model containment="true"
	 *        extendedMetaData="kind='element' name='Tasks'"
	 * @generated
	 */
	EList<Task> getTasks();

	/**
	 * Returns the value of the '<em><b>Task Tags</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.cocalimo.core.model.task.TaskTag}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Task Tags</em>' containment reference list.
	 * @see org.eclipse.efbt.cocalimo.core.model.task.TaskPackage#getTaskModule_TaskTags()
	 * @model containment="true"
	 * @generated
	 */
	EList<TaskTag> getTaskTags();

	/**
	 * Returns the value of the '<em><b>Sceanrio Tags</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.cocalimo.core.model.scenarios.ScenarioTag}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Sceanrio Tags</em>' containment reference list.
	 * @see org.eclipse.efbt.cocalimo.core.model.task.TaskPackage#getTaskModule_SceanrioTags()
	 * @model containment="true"
	 * @generated
	 */
	EList<ScenarioTag> getSceanrioTags();

} // TaskModule
