/**
 */
package org.eclipse.efbt.cocalimo.core.model.functionality_module.impl;

import org.eclipse.efbt.cocalimo.core.model.functionality_module.Functionality_modulePackage;
import org.eclipse.efbt.cocalimo.core.model.functionality_module.ScenarioSetFunctionalityModule;

import org.eclipse.efbt.cocalimo.core.model.scenarios.ScenarioSet;

import org.eclipse.emf.common.notify.Notification;
import org.eclipse.emf.common.notify.NotificationChain;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Scenario Set Functionality Module</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.functionality_module.impl.ScenarioSetFunctionalityModuleImpl#getScenarioSetChoice <em>Scenario Set Choice</em>}</li>
 * </ul>
 *
 * @generated
 */
public class ScenarioSetFunctionalityModuleImpl extends FunctionalityModuleSetImpl implements ScenarioSetFunctionalityModule
{
	/**
	 * The cached value of the '{@link #getScenarioSetChoice() <em>Scenario Set Choice</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getScenarioSetChoice()
	 * @generated
	 * @ordered
	 */
	protected ScenarioSet scenarioSetChoice;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected ScenarioSetFunctionalityModuleImpl()
	{
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass()
	{
		return Functionality_modulePackage.Literals.SCENARIO_SET_FUNCTIONALITY_MODULE;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public ScenarioSet getScenarioSetChoice()
	{
		return scenarioSetChoice;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public NotificationChain basicSetScenarioSetChoice(ScenarioSet newScenarioSetChoice, NotificationChain msgs)
	{
		ScenarioSet oldScenarioSetChoice = scenarioSetChoice;
		scenarioSetChoice = newScenarioSetChoice;
		if (eNotificationRequired()) {
			ENotificationImpl notification = new ENotificationImpl(this, Notification.SET, Functionality_modulePackage.SCENARIO_SET_FUNCTIONALITY_MODULE__SCENARIO_SET_CHOICE, oldScenarioSetChoice, newScenarioSetChoice);
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
	public void setScenarioSetChoice(ScenarioSet newScenarioSetChoice)
	{
		if (newScenarioSetChoice != scenarioSetChoice) {
			NotificationChain msgs = null;
			if (scenarioSetChoice != null)
				msgs = ((InternalEObject)scenarioSetChoice).eInverseRemove(this, EOPPOSITE_FEATURE_BASE - Functionality_modulePackage.SCENARIO_SET_FUNCTIONALITY_MODULE__SCENARIO_SET_CHOICE, null, msgs);
			if (newScenarioSetChoice != null)
				msgs = ((InternalEObject)newScenarioSetChoice).eInverseAdd(this, EOPPOSITE_FEATURE_BASE - Functionality_modulePackage.SCENARIO_SET_FUNCTIONALITY_MODULE__SCENARIO_SET_CHOICE, null, msgs);
			msgs = basicSetScenarioSetChoice(newScenarioSetChoice, msgs);
			if (msgs != null) msgs.dispatch();
		}
		else if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Functionality_modulePackage.SCENARIO_SET_FUNCTIONALITY_MODULE__SCENARIO_SET_CHOICE, newScenarioSetChoice, newScenarioSetChoice));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public NotificationChain eInverseRemove(InternalEObject otherEnd, int featureID, NotificationChain msgs)
	{
		switch (featureID) {
			case Functionality_modulePackage.SCENARIO_SET_FUNCTIONALITY_MODULE__SCENARIO_SET_CHOICE:
				return basicSetScenarioSetChoice(null, msgs);
		}
		return super.eInverseRemove(otherEnd, featureID, msgs);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object eGet(int featureID, boolean resolve, boolean coreType)
	{
		switch (featureID) {
			case Functionality_modulePackage.SCENARIO_SET_FUNCTIONALITY_MODULE__SCENARIO_SET_CHOICE:
				return getScenarioSetChoice();
		}
		return super.eGet(featureID, resolve, coreType);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void eSet(int featureID, Object newValue)
	{
		switch (featureID) {
			case Functionality_modulePackage.SCENARIO_SET_FUNCTIONALITY_MODULE__SCENARIO_SET_CHOICE:
				setScenarioSetChoice((ScenarioSet)newValue);
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
	public void eUnset(int featureID)
	{
		switch (featureID) {
			case Functionality_modulePackage.SCENARIO_SET_FUNCTIONALITY_MODULE__SCENARIO_SET_CHOICE:
				setScenarioSetChoice((ScenarioSet)null);
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
	public boolean eIsSet(int featureID)
	{
		switch (featureID) {
			case Functionality_modulePackage.SCENARIO_SET_FUNCTIONALITY_MODULE__SCENARIO_SET_CHOICE:
				return scenarioSetChoice != null;
		}
		return super.eIsSet(featureID);
	}

} //ScenarioSetFunctionalityModuleImpl
