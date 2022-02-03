/**
 */
package org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl;

import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.BasicFunction;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.BasicScenarioLogic;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.Ecore_plus_logicPackage;

import org.eclipse.efbt.cocalimo.core.model.logical_transformations.Scenario;

import org.eclipse.emf.common.notify.Notification;
import org.eclipse.emf.common.notify.NotificationChain;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;
import org.eclipse.emf.ecore.impl.MinimalEObjectImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Basic Scenario Logic</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.BasicScenarioLogicImpl#getBasicFunction <em>Basic Function</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.BasicScenarioLogicImpl#getSceanrio <em>Sceanrio</em>}</li>
 * </ul>
 *
 * @generated
 */
public class BasicScenarioLogicImpl extends MinimalEObjectImpl.Container implements BasicScenarioLogic {
	/**
	 * The cached value of the '{@link #getBasicFunction() <em>Basic Function</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getBasicFunction()
	 * @generated
	 * @ordered
	 */
	protected BasicFunction basicFunction;

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
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected BasicScenarioLogicImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return Ecore_plus_logicPackage.Literals.BASIC_SCENARIO_LOGIC;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public BasicFunction getBasicFunction() {
		return basicFunction;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public NotificationChain basicSetBasicFunction(BasicFunction newBasicFunction, NotificationChain msgs) {
		BasicFunction oldBasicFunction = basicFunction;
		basicFunction = newBasicFunction;
		if (eNotificationRequired()) {
			ENotificationImpl notification = new ENotificationImpl(this, Notification.SET, Ecore_plus_logicPackage.BASIC_SCENARIO_LOGIC__BASIC_FUNCTION, oldBasicFunction, newBasicFunction);
			if (msgs == null) msgs = notification; else msgs.add(notification);
		}
		return msgs;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setBasicFunction(BasicFunction newBasicFunction) {
		if (newBasicFunction != basicFunction) {
			NotificationChain msgs = null;
			if (basicFunction != null)
				msgs = ((InternalEObject)basicFunction).eInverseRemove(this, EOPPOSITE_FEATURE_BASE - Ecore_plus_logicPackage.BASIC_SCENARIO_LOGIC__BASIC_FUNCTION, null, msgs);
			if (newBasicFunction != null)
				msgs = ((InternalEObject)newBasicFunction).eInverseAdd(this, EOPPOSITE_FEATURE_BASE - Ecore_plus_logicPackage.BASIC_SCENARIO_LOGIC__BASIC_FUNCTION, null, msgs);
			msgs = basicSetBasicFunction(newBasicFunction, msgs);
			if (msgs != null) msgs.dispatch();
		}
		else if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Ecore_plus_logicPackage.BASIC_SCENARIO_LOGIC__BASIC_FUNCTION, newBasicFunction, newBasicFunction));
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
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Ecore_plus_logicPackage.BASIC_SCENARIO_LOGIC__SCEANRIO, oldSceanrio, sceanrio));
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
			eNotify(new ENotificationImpl(this, Notification.SET, Ecore_plus_logicPackage.BASIC_SCENARIO_LOGIC__SCEANRIO, oldSceanrio, sceanrio));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public NotificationChain eInverseRemove(InternalEObject otherEnd, int featureID, NotificationChain msgs) {
		switch (featureID) {
			case Ecore_plus_logicPackage.BASIC_SCENARIO_LOGIC__BASIC_FUNCTION:
				return basicSetBasicFunction(null, msgs);
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
			case Ecore_plus_logicPackage.BASIC_SCENARIO_LOGIC__BASIC_FUNCTION:
				return getBasicFunction();
			case Ecore_plus_logicPackage.BASIC_SCENARIO_LOGIC__SCEANRIO:
				if (resolve) return getSceanrio();
				return basicGetSceanrio();
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
			case Ecore_plus_logicPackage.BASIC_SCENARIO_LOGIC__BASIC_FUNCTION:
				setBasicFunction((BasicFunction)newValue);
				return;
			case Ecore_plus_logicPackage.BASIC_SCENARIO_LOGIC__SCEANRIO:
				setSceanrio((Scenario)newValue);
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
			case Ecore_plus_logicPackage.BASIC_SCENARIO_LOGIC__BASIC_FUNCTION:
				setBasicFunction((BasicFunction)null);
				return;
			case Ecore_plus_logicPackage.BASIC_SCENARIO_LOGIC__SCEANRIO:
				setSceanrio((Scenario)null);
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
			case Ecore_plus_logicPackage.BASIC_SCENARIO_LOGIC__BASIC_FUNCTION:
				return basicFunction != null;
			case Ecore_plus_logicPackage.BASIC_SCENARIO_LOGIC__SCEANRIO:
				return sceanrio != null;
		}
		return super.eIsSet(featureID);
	}

} //BasicScenarioLogicImpl
