/**
 */
package org.eclipse.efbt.cocalimo.core.model.logical_transformations.impl;

import java.util.Collection;

import org.eclipse.efbt.cocalimo.core.model.logical_transformations.Logical_transformationsPackage;
import org.eclipse.efbt.cocalimo.core.model.logical_transformations.Scenario;
import org.eclipse.efbt.cocalimo.core.model.logical_transformations.Test;

import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;
import org.eclipse.emf.ecore.impl.MinimalEObjectImpl;

import org.eclipse.emf.ecore.util.EObjectResolvingEList;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Test</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.logical_transformations.impl.TestImpl#getScenarios <em>Scenarios</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.logical_transformations.impl.TestImpl#getInputData <em>Input Data</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.logical_transformations.impl.TestImpl#getExpectedResult <em>Expected Result</em>}</li>
 * </ul>
 *
 * @generated
 */
public class TestImpl extends MinimalEObjectImpl.Container implements Test {
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
	 * The cached value of the '{@link #getInputData() <em>Input Data</em>}' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getInputData()
	 * @generated
	 * @ordered
	 */
	protected EList<EObject> inputData;

	/**
	 * The cached value of the '{@link #getExpectedResult() <em>Expected Result</em>}' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getExpectedResult()
	 * @generated
	 * @ordered
	 */
	protected EList<EObject> expectedResult;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected TestImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return Logical_transformationsPackage.Literals.TEST;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Scenario getScenarios() {
		if (scenarios != null && scenarios.eIsProxy()) {
			InternalEObject oldScenarios = (InternalEObject)scenarios;
			scenarios = (Scenario)eResolveProxy(oldScenarios);
			if (scenarios != oldScenarios) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Logical_transformationsPackage.TEST__SCENARIOS, oldScenarios, scenarios));
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
	@Override
	public void setScenarios(Scenario newScenarios) {
		Scenario oldScenarios = scenarios;
		scenarios = newScenarios;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Logical_transformationsPackage.TEST__SCENARIOS, oldScenarios, scenarios));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EList<EObject> getInputData() {
		if (inputData == null) {
			inputData = new EObjectResolvingEList<EObject>(EObject.class, this, Logical_transformationsPackage.TEST__INPUT_DATA);
		}
		return inputData;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EList<EObject> getExpectedResult() {
		if (expectedResult == null) {
			expectedResult = new EObjectResolvingEList<EObject>(EObject.class, this, Logical_transformationsPackage.TEST__EXPECTED_RESULT);
		}
		return expectedResult;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object eGet(int featureID, boolean resolve, boolean coreType) {
		switch (featureID) {
			case Logical_transformationsPackage.TEST__SCENARIOS:
				if (resolve) return getScenarios();
				return basicGetScenarios();
			case Logical_transformationsPackage.TEST__INPUT_DATA:
				return getInputData();
			case Logical_transformationsPackage.TEST__EXPECTED_RESULT:
				return getExpectedResult();
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
			case Logical_transformationsPackage.TEST__SCENARIOS:
				setScenarios((Scenario)newValue);
				return;
			case Logical_transformationsPackage.TEST__INPUT_DATA:
				getInputData().clear();
				getInputData().addAll((Collection<? extends EObject>)newValue);
				return;
			case Logical_transformationsPackage.TEST__EXPECTED_RESULT:
				getExpectedResult().clear();
				getExpectedResult().addAll((Collection<? extends EObject>)newValue);
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
			case Logical_transformationsPackage.TEST__SCENARIOS:
				setScenarios((Scenario)null);
				return;
			case Logical_transformationsPackage.TEST__INPUT_DATA:
				getInputData().clear();
				return;
			case Logical_transformationsPackage.TEST__EXPECTED_RESULT:
				getExpectedResult().clear();
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
			case Logical_transformationsPackage.TEST__SCENARIOS:
				return scenarios != null;
			case Logical_transformationsPackage.TEST__INPUT_DATA:
				return inputData != null && !inputData.isEmpty();
			case Logical_transformationsPackage.TEST__EXPECTED_RESULT:
				return expectedResult != null && !expectedResult.isEmpty();
		}
		return super.eIsSet(featureID);
	}

} //TestImpl
