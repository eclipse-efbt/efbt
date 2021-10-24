/**
 */
package org.eclipse.efbt.cocalimo.core.model.task;

import org.eclipse.emf.common.util.EList;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Set</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * A set of Tasks.
 * Note that TasksSet is also a subclass of Task, so the set can also include TasksSets
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.task.TaskSet#getSubTasks <em>Sub Tasks</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.core.model.task.TaskPackage#getTaskSet()
 * @model extendedMetaData="name='TaskSet' kind='elementOnly'"
 * @generated
 */
public interface TaskSet extends Task {
	/**
	 * Returns the value of the '<em><b>Sub Tasks</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.cocalimo.core.model.task.Task}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The  contained set of Tasks 
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Sub Tasks</em>' containment reference list.
	 * @see org.eclipse.efbt.cocalimo.core.model.task.TaskPackage#getTaskSet_SubTasks()
	 * @model containment="true"
	 *        extendedMetaData="kind='element' name='subTasks'"
	 * @generated
	 */
	EList<Task> getSubTasks();

} // TaskSet
