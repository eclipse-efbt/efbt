/**
 */
package org.eclipse.efbt.cocalimo.core.model.scenarios.impl;

import org.eclipse.efbt.cocalimo.core.model.requirements_text.impl.TagImpl;

import org.eclipse.efbt.cocalimo.core.model.scenarios.ScenarioSet;
import org.eclipse.efbt.cocalimo.core.model.scenarios.ScenarioSetTag;
import org.eclipse.efbt.cocalimo.core.model.scenarios.ScenariosPackage;

import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Scenario Set Tag</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.scenarios.impl.ScenarioSetTagImpl#getScenarioSet <em>Scenario Set</em>}</li>
 * </ul>
 *
 * @generated
 */
public class ScenarioSetTagImpl extends TagImpl implements ScenarioSetTag {
	/**
	 * The cached value of the '{@link #getScenarioSet() <em>Scenario Set</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getScenarioSet()
	 * @generated
	 * @ordered
	 */
	protected ScenarioSet scenarioSet;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected ScenarioSetTagImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return ScenariosPackage.Literals.SCENARIO_SET_TAG;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public ScenarioSet getScenarioSet() {
		if (scenarioSet != null && scenarioSet.eIsProxy()) {
			InternalEObject oldScenarioSet = (InternalEObject)scenarioSet;
			scenarioSet = (ScenarioSet)eResolveProxy(oldScenarioSet);
			if (scenarioSet != oldScenarioSet) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, ScenariosPackage.SCENARIO_SET_TAG__SCENARIO_SET, oldScenarioSet, scenarioSet));
			}
		}
		return scenarioSet;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public ScenarioSet basicGetScenarioSet() {
		return scenarioSet;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setScenarioSet(ScenarioSet newScenarioSet) {
		ScenarioSet oldScenarioSet = scenarioSet;
		scenarioSet = newScenarioSet;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, ScenariosPackage.SCENARIO_SET_TAG__SCENARIO_SET, oldScenarioSet, scenarioSet));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object eGet(int featureID, boolean resolve, boolean coreType) {
		switch (featureID) {
			case ScenariosPackage.SCENARIO_SET_TAG__SCENARIO_SET:
				if (resolve) return getScenarioSet();
				return basicGetScenarioSet();
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
			case ScenariosPackage.SCENARIO_SET_TAG__SCENARIO_SET:
				setScenarioSet((ScenarioSet)newValue);
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
			case ScenariosPackage.SCENARIO_SET_TAG__SCENARIO_SET:
				setScenarioSet((ScenarioSet)null);
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
			case ScenariosPackage.SCENARIO_SET_TAG__SCENARIO_SET:
				return scenarioSet != null;
		}
		return super.eIsSet(featureID);
	}

} //ScenarioSetTagImpl
