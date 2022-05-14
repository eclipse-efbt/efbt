/**
 */
package org.eclipse.efbt.openregspecs.model.testing.impl;

import org.eclipse.efbt.openregspecs.model.testing.Scenario;
import org.eclipse.efbt.openregspecs.model.testing.TestingPackage;
import org.eclipse.efbt.openregspecs.model.testing.UnitTestScope;

import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Unit Test Scope</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.testing.impl.UnitTestScopeImpl#getScenarios <em>Scenarios</em>}</li>
 * </ul>
 *
 * @generated
 */
public class UnitTestScopeImpl extends TestScopeImpl implements UnitTestScope {
	/**
	 * The cached value of the '{@link #getScenarios() <em>Scenarios</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getScenarios()
	 * @generated
	 * @ordered
	 */
	protected Scenario scenarios;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected UnitTestScopeImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return TestingPackage.Literals.UNIT_TEST_SCOPE;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Scenario getScenarios() {
		if (scenarios != null && scenarios.eIsProxy()) {
			InternalEObject oldScenarios = (InternalEObject)scenarios;
			scenarios = (Scenario)eResolveProxy(oldScenarios);
			if (scenarios != oldScenarios) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, TestingPackage.UNIT_TEST_SCOPE__SCENARIOS, oldScenarios, scenarios));
			}
		}
		return scenarios;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Scenario basicGetScenarios() {
		return scenarios;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setScenarios(Scenario newScenarios) {
		Scenario oldScenarios = scenarios;
		scenarios = newScenarios;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, TestingPackage.UNIT_TEST_SCOPE__SCENARIOS, oldScenarios, scenarios));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object eGet(int featureID, boolean resolve, boolean coreType) {
		switch (featureID) {
			case TestingPackage.UNIT_TEST_SCOPE__SCENARIOS:
				if (resolve) return getScenarios();
				return basicGetScenarios();
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
			case TestingPackage.UNIT_TEST_SCOPE__SCENARIOS:
				setScenarios((Scenario)newValue);
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
			case TestingPackage.UNIT_TEST_SCOPE__SCENARIOS:
				setScenarios((Scenario)null);
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
			case TestingPackage.UNIT_TEST_SCOPE__SCENARIOS:
				return scenarios != null;
		}
		return super.eIsSet(featureID);
	}

} //UnitTestScopeImpl
