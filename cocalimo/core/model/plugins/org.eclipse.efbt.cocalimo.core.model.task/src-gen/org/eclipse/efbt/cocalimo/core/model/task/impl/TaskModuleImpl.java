/**
 */
package org.eclipse.efbt.cocalimo.core.model.task.impl;

import java.util.Collection;

import org.eclipse.efbt.cocalimo.core.model.module_management.impl.ModuleImpl;

import org.eclipse.efbt.cocalimo.core.model.scenarios.ScenarioTag;

import org.eclipse.efbt.cocalimo.core.model.task.Task;
import org.eclipse.efbt.cocalimo.core.model.task.TaskModule;
import org.eclipse.efbt.cocalimo.core.model.task.TaskPackage;
import org.eclipse.efbt.cocalimo.core.model.task.TaskTag;

import org.eclipse.emf.common.notify.NotificationChain;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.util.EObjectContainmentEList;
import org.eclipse.emf.ecore.util.InternalEList;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Module</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.task.impl.TaskModuleImpl#getTasks <em>Tasks</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.task.impl.TaskModuleImpl#getTaskTags <em>Task Tags</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.task.impl.TaskModuleImpl#getSceanrioTags <em>Sceanrio Tags</em>}</li>
 * </ul>
 *
 * @generated
 */
public class TaskModuleImpl extends ModuleImpl implements TaskModule {
	/**
	 * The cached value of the '{@link #getTasks() <em>Tasks</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getTasks()
	 * @generated
	 * @ordered
	 */
	protected EList<Task> tasks;

	/**
	 * The cached value of the '{@link #getTaskTags() <em>Task Tags</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getTaskTags()
	 * @generated
	 * @ordered
	 */
	protected EList<TaskTag> taskTags;

	/**
	 * The cached value of the '{@link #getSceanrioTags() <em>Sceanrio Tags</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getSceanrioTags()
	 * @generated
	 * @ordered
	 */
	protected EList<ScenarioTag> sceanrioTags;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected TaskModuleImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return TaskPackage.Literals.TASK_MODULE;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EList<Task> getTasks() {
		if (tasks == null) {
			tasks = new EObjectContainmentEList<Task>(Task.class, this, TaskPackage.TASK_MODULE__TASKS);
		}
		return tasks;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EList<TaskTag> getTaskTags() {
		if (taskTags == null) {
			taskTags = new EObjectContainmentEList<TaskTag>(TaskTag.class, this, TaskPackage.TASK_MODULE__TASK_TAGS);
		}
		return taskTags;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EList<ScenarioTag> getSceanrioTags() {
		if (sceanrioTags == null) {
			sceanrioTags = new EObjectContainmentEList<ScenarioTag>(ScenarioTag.class, this, TaskPackage.TASK_MODULE__SCEANRIO_TAGS);
		}
		return sceanrioTags;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public NotificationChain eInverseRemove(InternalEObject otherEnd, int featureID, NotificationChain msgs) {
		switch (featureID) {
			case TaskPackage.TASK_MODULE__TASKS:
				return ((InternalEList<?>)getTasks()).basicRemove(otherEnd, msgs);
			case TaskPackage.TASK_MODULE__TASK_TAGS:
				return ((InternalEList<?>)getTaskTags()).basicRemove(otherEnd, msgs);
			case TaskPackage.TASK_MODULE__SCEANRIO_TAGS:
				return ((InternalEList<?>)getSceanrioTags()).basicRemove(otherEnd, msgs);
		}
		return super.eInverseRemove(otherEnd, featureID, msgs);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object eGet(int featureID, boolean resolve, boolean coreType) {
		switch (featureID) {
			case TaskPackage.TASK_MODULE__TASKS:
				return getTasks();
			case TaskPackage.TASK_MODULE__TASK_TAGS:
				return getTaskTags();
			case TaskPackage.TASK_MODULE__SCEANRIO_TAGS:
				return getSceanrioTags();
		}
		return super.eGet(featureID, resolve, coreType);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@SuppressWarnings("unchecked")
	@Override
	public void eSet(int featureID, Object newValue) {
		switch (featureID) {
			case TaskPackage.TASK_MODULE__TASKS:
				getTasks().clear();
				getTasks().addAll((Collection<? extends Task>)newValue);
				return;
			case TaskPackage.TASK_MODULE__TASK_TAGS:
				getTaskTags().clear();
				getTaskTags().addAll((Collection<? extends TaskTag>)newValue);
				return;
			case TaskPackage.TASK_MODULE__SCEANRIO_TAGS:
				getSceanrioTags().clear();
				getSceanrioTags().addAll((Collection<? extends ScenarioTag>)newValue);
				return;
		}
		super.eSet(featureID, newValue);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void eUnset(int featureID) {
		switch (featureID) {
			case TaskPackage.TASK_MODULE__TASKS:
				getTasks().clear();
				return;
			case TaskPackage.TASK_MODULE__TASK_TAGS:
				getTaskTags().clear();
				return;
			case TaskPackage.TASK_MODULE__SCEANRIO_TAGS:
				getSceanrioTags().clear();
				return;
		}
		super.eUnset(featureID);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public boolean eIsSet(int featureID) {
		switch (featureID) {
			case TaskPackage.TASK_MODULE__TASKS:
				return tasks != null && !tasks.isEmpty();
			case TaskPackage.TASK_MODULE__TASK_TAGS:
				return taskTags != null && !taskTags.isEmpty();
			case TaskPackage.TASK_MODULE__SCEANRIO_TAGS:
				return sceanrioTags != null && !sceanrioTags.isEmpty();
		}
		return super.eIsSet(featureID);
	}

} //TaskModuleImpl
