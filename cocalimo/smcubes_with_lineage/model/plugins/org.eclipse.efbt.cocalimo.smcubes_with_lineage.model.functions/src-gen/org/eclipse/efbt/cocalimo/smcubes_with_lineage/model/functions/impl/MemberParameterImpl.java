/**
 */
package org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.impl;

import org.eclipse.efbt.cocalimo.smcubes.model.core.MEMBER;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.FunctionsPackage;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.MemberParameter;

import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Member Parameter</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.impl.MemberParameterImpl#getParam <em>Param</em>}</li>
 * </ul>
 *
 * @generated
 */
public class MemberParameterImpl extends ParameterImpl implements MemberParameter {
	/**
	 * The cached value of the '{@link #getParam() <em>Param</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getParam()
	 * @generated
	 * @ordered
	 */
	protected MEMBER param;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected MemberParameterImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return FunctionsPackage.Literals.MEMBER_PARAMETER;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public MEMBER getParam() {
		if (param != null && param.eIsProxy()) {
			InternalEObject oldParam = (InternalEObject)param;
			param = (MEMBER)eResolveProxy(oldParam);
			if (param != oldParam) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, FunctionsPackage.MEMBER_PARAMETER__PARAM, oldParam, param));
			}
		}
		return param;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public MEMBER basicGetParam() {
		return param;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setParam(MEMBER newParam) {
		MEMBER oldParam = param;
		param = newParam;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, FunctionsPackage.MEMBER_PARAMETER__PARAM, oldParam, param));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object eGet(int featureID, boolean resolve, boolean coreType) {
		switch (featureID) {
			case FunctionsPackage.MEMBER_PARAMETER__PARAM:
				if (resolve) return getParam();
				return basicGetParam();
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
			case FunctionsPackage.MEMBER_PARAMETER__PARAM:
				setParam((MEMBER)newValue);
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
			case FunctionsPackage.MEMBER_PARAMETER__PARAM:
				setParam((MEMBER)null);
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
			case FunctionsPackage.MEMBER_PARAMETER__PARAM:
				return param != null;
		}
		return super.eIsSet(featureID);
	}

} //MemberParameterImpl
