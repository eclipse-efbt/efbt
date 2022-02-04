/**
 */
package org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl;

import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.BasicFunctionSpec;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.BasicScenarioOperation;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.Ecore_plus_logicPackage;

import org.eclipse.efbt.cocalimo.core.model.logical_transformations.Scenario;

import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Basic Scenario Operation</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.BasicScenarioOperationImpl#getSceanrio <em>Sceanrio</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.BasicScenarioOperationImpl#getFunctionSpec <em>Function Spec</em>}</li>
 * </ul>
 *
 * @generated
 */
public class BasicScenarioOperationImpl extends OperationImpl implements BasicScenarioOperation {
	/**
	 * The cached value of the '{@link #getSceanrio() <em>Sceanrio</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getSceanrio()
	 * @generated
	 * @ordered
	 */
	protected Scenario sceanrio;

	/**
	 * The cached value of the '{@link #getFunctionSpec() <em>Function Spec</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getFunctionSpec()
	 * @generated
	 * @ordered
	 */
	protected BasicFunctionSpec functionSpec;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected BasicScenarioOperationImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return Ecore_plus_logicPackage.Literals.BASIC_SCENARIO_OPERATION;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Scenario getSceanrio() {
		if (sceanrio != null && sceanrio.eIsProxy()) {
			InternalEObject oldSceanrio = (InternalEObject)sceanrio;
			sceanrio = (Scenario)eResolveProxy(oldSceanrio);
			if (sceanrio != oldSceanrio) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Ecore_plus_logicPackage.BASIC_SCENARIO_OPERATION__SCEANRIO, oldSceanrio, sceanrio));
			}
		}
		return sceanrio;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Scenario basicGetSceanrio() {
		return sceanrio;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setSceanrio(Scenario newSceanrio) {
		Scenario oldSceanrio = sceanrio;
		sceanrio = newSceanrio;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Ecore_plus_logicPackage.BASIC_SCENARIO_OPERATION__SCEANRIO, oldSceanrio, sceanrio));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public BasicFunctionSpec getFunctionSpec() {
		if (functionSpec != null && functionSpec.eIsProxy()) {
			InternalEObject oldFunctionSpec = (InternalEObject)functionSpec;
			functionSpec = (BasicFunctionSpec)eResolveProxy(oldFunctionSpec);
			if (functionSpec != oldFunctionSpec) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Ecore_plus_logicPackage.BASIC_SCENARIO_OPERATION__FUNCTION_SPEC, oldFunctionSpec, functionSpec));
			}
		}
		return functionSpec;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public BasicFunctionSpec basicGetFunctionSpec() {
		return functionSpec;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setFunctionSpec(BasicFunctionSpec newFunctionSpec) {
		BasicFunctionSpec oldFunctionSpec = functionSpec;
		functionSpec = newFunctionSpec;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Ecore_plus_logicPackage.BASIC_SCENARIO_OPERATION__FUNCTION_SPEC, oldFunctionSpec, functionSpec));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object eGet(int featureID, boolean resolve, boolean coreType) {
		switch (featureID) {
			case Ecore_plus_logicPackage.BASIC_SCENARIO_OPERATION__SCEANRIO:
				if (resolve) return getSceanrio();
				return basicGetSceanrio();
			case Ecore_plus_logicPackage.BASIC_SCENARIO_OPERATION__FUNCTION_SPEC:
				if (resolve) return getFunctionSpec();
				return basicGetFunctionSpec();
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
			case Ecore_plus_logicPackage.BASIC_SCENARIO_OPERATION__SCEANRIO:
				setSceanrio((Scenario)newValue);
				return;
			case Ecore_plus_logicPackage.BASIC_SCENARIO_OPERATION__FUNCTION_SPEC:
				setFunctionSpec((BasicFunctionSpec)newValue);
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
			case Ecore_plus_logicPackage.BASIC_SCENARIO_OPERATION__SCEANRIO:
				setSceanrio((Scenario)null);
				return;
			case Ecore_plus_logicPackage.BASIC_SCENARIO_OPERATION__FUNCTION_SPEC:
				setFunctionSpec((BasicFunctionSpec)null);
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
			case Ecore_plus_logicPackage.BASIC_SCENARIO_OPERATION__SCEANRIO:
				return sceanrio != null;
			case Ecore_plus_logicPackage.BASIC_SCENARIO_OPERATION__FUNCTION_SPEC:
				return functionSpec != null;
		}
		return super.eIsSet(featureID);
	}

} //BasicScenarioOperationImpl
