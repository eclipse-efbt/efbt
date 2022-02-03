/**
 */
package org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl;

import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.BooleanFunction;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.Ecore_plus_logicPackage;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.RowJoinFunction;

import org.eclipse.emf.common.notify.Notification;
import org.eclipse.emf.common.notify.NotificationChain;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Row Join Function</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.RowJoinFunctionImpl#getJoinFunction <em>Join Function</em>}</li>
 * </ul>
 *
 * @generated
 */
public class RowJoinFunctionImpl extends RowCreationApproachImpl implements RowJoinFunction {
	/**
	 * The cached value of the '{@link #getJoinFunction() <em>Join Function</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getJoinFunction()
	 * @generated
	 * @ordered
	 */
	protected BooleanFunction joinFunction;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected RowJoinFunctionImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return Ecore_plus_logicPackage.Literals.ROW_JOIN_FUNCTION;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public BooleanFunction getJoinFunction() {
		return joinFunction;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public NotificationChain basicSetJoinFunction(BooleanFunction newJoinFunction, NotificationChain msgs) {
		BooleanFunction oldJoinFunction = joinFunction;
		joinFunction = newJoinFunction;
		if (eNotificationRequired()) {
			ENotificationImpl notification = new ENotificationImpl(this, Notification.SET, Ecore_plus_logicPackage.ROW_JOIN_FUNCTION__JOIN_FUNCTION, oldJoinFunction, newJoinFunction);
			if (msgs == null) msgs = notification; else msgs.add(notification);
		}
		return msgs;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setJoinFunction(BooleanFunction newJoinFunction) {
		if (newJoinFunction != joinFunction) {
			NotificationChain msgs = null;
			if (joinFunction != null)
				msgs = ((InternalEObject)joinFunction).eInverseRemove(this, EOPPOSITE_FEATURE_BASE - Ecore_plus_logicPackage.ROW_JOIN_FUNCTION__JOIN_FUNCTION, null, msgs);
			if (newJoinFunction != null)
				msgs = ((InternalEObject)newJoinFunction).eInverseAdd(this, EOPPOSITE_FEATURE_BASE - Ecore_plus_logicPackage.ROW_JOIN_FUNCTION__JOIN_FUNCTION, null, msgs);
			msgs = basicSetJoinFunction(newJoinFunction, msgs);
			if (msgs != null) msgs.dispatch();
		}
		else if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Ecore_plus_logicPackage.ROW_JOIN_FUNCTION__JOIN_FUNCTION, newJoinFunction, newJoinFunction));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public NotificationChain eInverseRemove(InternalEObject otherEnd, int featureID, NotificationChain msgs) {
		switch (featureID) {
			case Ecore_plus_logicPackage.ROW_JOIN_FUNCTION__JOIN_FUNCTION:
				return basicSetJoinFunction(null, msgs);
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
			case Ecore_plus_logicPackage.ROW_JOIN_FUNCTION__JOIN_FUNCTION:
				return getJoinFunction();
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
			case Ecore_plus_logicPackage.ROW_JOIN_FUNCTION__JOIN_FUNCTION:
				setJoinFunction((BooleanFunction)newValue);
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
			case Ecore_plus_logicPackage.ROW_JOIN_FUNCTION__JOIN_FUNCTION:
				setJoinFunction((BooleanFunction)null);
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
			case Ecore_plus_logicPackage.ROW_JOIN_FUNCTION__JOIN_FUNCTION:
				return joinFunction != null;
		}
		return super.eIsSet(featureID);
	}

} //RowJoinFunctionImpl
