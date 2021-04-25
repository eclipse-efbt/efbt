/**
 */
package org.eclipse.efbt.cocalimo.core.model.test_definition.impl;

import org.eclipse.efbt.cocalimo.core.model.test_definition.Param;
import org.eclipse.efbt.cocalimo.core.model.test_definition.Test_definitionPackage;

import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;
import org.eclipse.emf.ecore.impl.MinimalEObjectImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Param</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.test_definition.impl.ParamImpl#getParam <em>Param</em>}</li>
 * </ul>
 *
 * @generated
 */
public class ParamImpl extends MinimalEObjectImpl.Container implements Param
{
	/**
	 * The cached value of the '{@link #getParam() <em>Param</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getParam()
	 * @generated
	 * @ordered
	 */
	protected EObject param;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected ParamImpl()
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
		return Test_definitionPackage.Literals.PARAM;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EObject getParam()
	{
		if (param != null && param.eIsProxy()) {
			InternalEObject oldParam = (InternalEObject)param;
			param = eResolveProxy(oldParam);
			if (param != oldParam) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Test_definitionPackage.PARAM__PARAM, oldParam, param));
			}
		}
		return param;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EObject basicGetParam()
	{
		return param;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setParam(EObject newParam)
	{
		EObject oldParam = param;
		param = newParam;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Test_definitionPackage.PARAM__PARAM, oldParam, param));
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
			case Test_definitionPackage.PARAM__PARAM:
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
	public void eSet(int featureID, Object newValue)
	{
		switch (featureID) {
			case Test_definitionPackage.PARAM__PARAM:
				setParam((EObject)newValue);
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
			case Test_definitionPackage.PARAM__PARAM:
				setParam((EObject)null);
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
			case Test_definitionPackage.PARAM__PARAM:
				return param != null;
		}
		return super.eIsSet(featureID);
	}

} //ParamImpl
