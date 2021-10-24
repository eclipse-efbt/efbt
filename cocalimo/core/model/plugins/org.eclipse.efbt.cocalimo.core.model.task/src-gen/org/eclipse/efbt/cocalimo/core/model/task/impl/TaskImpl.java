/**
 */
package org.eclipse.efbt.cocalimo.core.model.task.impl;

import org.eclipse.efbt.cocalimo.core.model.scenarios.Scenario;
import org.eclipse.efbt.cocalimo.core.model.scenarios.ScenarioSet;

import org.eclipse.efbt.cocalimo.core.model.task.Task;
import org.eclipse.efbt.cocalimo.core.model.task.TaskPackage;

import org.eclipse.emf.common.notify.Notification;
import org.eclipse.emf.common.notify.NotificationChain;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;
import org.eclipse.emf.ecore.impl.MinimalEObjectImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Task</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.task.impl.TaskImpl#getName <em>Name</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.task.impl.TaskImpl#getScenario <em>Scenario</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.task.impl.TaskImpl#getScenarioSet <em>Scenario Set</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.task.impl.TaskImpl#isUsedInSubset <em>Used In Subset</em>}</li>
 * </ul>
 *
 * @generated
 */
public abstract class TaskImpl extends MinimalEObjectImpl.Container implements Task {
	/**
	 * The default value of the '{@link #getName() <em>Name</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getName()
	 * @generated
	 * @ordered
	 */
	protected static final String NAME_EDEFAULT = null;

	/**
	 * The cached value of the '{@link #getName() <em>Name</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getName()
	 * @generated
	 * @ordered
	 */
	protected String name = NAME_EDEFAULT;

	/**
	 * The cached value of the '{@link #getScenario() <em>Scenario</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getScenario()
	 * @generated
	 * @ordered
	 */
	protected Scenario scenario;

	/**
	 * The cached value of the '{@link #getScenarioSet() <em>Scenario Set</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getScenarioSet()
	 * @generated
	 * @ordered
	 */
	protected ScenarioSet scenarioSet;

	/**
	 * The default value of the '{@link #isUsedInSubset() <em>Used In Subset</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #isUsedInSubset()
	 * @generated
	 * @ordered
	 */
	protected static final boolean USED_IN_SUBSET_EDEFAULT = false;

	/**
	 * The cached value of the '{@link #isUsedInSubset() <em>Used In Subset</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #isUsedInSubset()
	 * @generated
	 * @ordered
	 */
	protected boolean usedInSubset = USED_IN_SUBSET_EDEFAULT;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected TaskImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return TaskPackage.Literals.TASK;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public String getName() {
		return name;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setName(String newName) {
		String oldName = name;
		name = newName;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, TaskPackage.TASK__NAME, oldName, name));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Scenario getScenario() {
		if (scenario != null && scenario.eIsProxy()) {
			InternalEObject oldScenario = (InternalEObject)scenario;
			scenario = (Scenario)eResolveProxy(oldScenario);
			if (scenario != oldScenario) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, TaskPackage.TASK__SCENARIO, oldScenario, scenario));
			}
		}
		return scenario;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Scenario basicGetScenario() {
		return scenario;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setScenario(Scenario newScenario) {
		Scenario oldScenario = scenario;
		scenario = newScenario;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, TaskPackage.TASK__SCENARIO, oldScenario, scenario));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public ScenarioSet getScenarioSet() {
		return scenarioSet;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public NotificationChain basicSetScenarioSet(ScenarioSet newScenarioSet, NotificationChain msgs) {
		ScenarioSet oldScenarioSet = scenarioSet;
		scenarioSet = newScenarioSet;
		if (eNotificationRequired()) {
			ENotificationImpl notification = new ENotificationImpl(this, Notification.SET, TaskPackage.TASK__SCENARIO_SET, oldScenarioSet, newScenarioSet);
			if (msgs == null) msgs = notification; else msgs.add(notification);
		}
		return msgs;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setScenarioSet(ScenarioSet newScenarioSet) {
		if (newScenarioSet != scenarioSet) {
			NotificationChain msgs = null;
			if (scenarioSet != null)
				msgs = ((InternalEObject)scenarioSet).eInverseRemove(this, EOPPOSITE_FEATURE_BASE - TaskPackage.TASK__SCENARIO_SET, null, msgs);
			if (newScenarioSet != null)
				msgs = ((InternalEObject)newScenarioSet).eInverseAdd(this, EOPPOSITE_FEATURE_BASE - TaskPackage.TASK__SCENARIO_SET, null, msgs);
			msgs = basicSetScenarioSet(newScenarioSet, msgs);
			if (msgs != null) msgs.dispatch();
		}
		else if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, TaskPackage.TASK__SCENARIO_SET, newScenarioSet, newScenarioSet));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public boolean isUsedInSubset() {
		return usedInSubset;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setUsedInSubset(boolean newUsedInSubset) {
		boolean oldUsedInSubset = usedInSubset;
		usedInSubset = newUsedInSubset;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, TaskPackage.TASK__USED_IN_SUBSET, oldUsedInSubset, usedInSubset));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public NotificationChain eInverseRemove(InternalEObject otherEnd, int featureID, NotificationChain msgs) {
		switch (featureID) {
			case TaskPackage.TASK__SCENARIO_SET:
				return basicSetScenarioSet(null, msgs);
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
			case TaskPackage.TASK__NAME:
				return getName();
			case TaskPackage.TASK__SCENARIO:
				if (resolve) return getScenario();
				return basicGetScenario();
			case TaskPackage.TASK__SCENARIO_SET:
				return getScenarioSet();
			case TaskPackage.TASK__USED_IN_SUBSET:
				return isUsedInSubset();
		}
		return super.eGet(featureID, resolve, coreType);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void eSet(int featureID, Object newValue) {
		switch (featureID) {
			case TaskPackage.TASK__NAME:
				setName((String)newValue);
				return;
			case TaskPackage.TASK__SCENARIO:
				setScenario((Scenario)newValue);
				return;
			case TaskPackage.TASK__SCENARIO_SET:
				setScenarioSet((ScenarioSet)newValue);
				return;
			case TaskPackage.TASK__USED_IN_SUBSET:
				setUsedInSubset((Boolean)newValue);
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
			case TaskPackage.TASK__NAME:
				setName(NAME_EDEFAULT);
				return;
			case TaskPackage.TASK__SCENARIO:
				setScenario((Scenario)null);
				return;
			case TaskPackage.TASK__SCENARIO_SET:
				setScenarioSet((ScenarioSet)null);
				return;
			case TaskPackage.TASK__USED_IN_SUBSET:
				setUsedInSubset(USED_IN_SUBSET_EDEFAULT);
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
			case TaskPackage.TASK__NAME:
				return NAME_EDEFAULT == null ? name != null : !NAME_EDEFAULT.equals(name);
			case TaskPackage.TASK__SCENARIO:
				return scenario != null;
			case TaskPackage.TASK__SCENARIO_SET:
				return scenarioSet != null;
			case TaskPackage.TASK__USED_IN_SUBSET:
				return usedInSubset != USED_IN_SUBSET_EDEFAULT;
		}
		return super.eIsSet(featureID);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public String toString() {
		if (eIsProxy()) return super.toString();

		StringBuilder result = new StringBuilder(super.toString());
		result.append(" (name: ");
		result.append(name);
		result.append(", usedInSubset: ");
		result.append(usedInSubset);
		result.append(')');
		return result.toString();
	}

} //TaskImpl
