/**
 */
package org.eclipse.efbt.controller.core.model.platform_call.impl;

import java.util.Collection;

import org.eclipse.efbt.common.model.module_management.impl.ModuleImpl;

import org.eclipse.efbt.controller.core.model.platform_call.PlatformCall;
import org.eclipse.efbt.controller.core.model.platform_call.PlatformCallModule;
import org.eclipse.efbt.controller.core.model.platform_call.Platform_callPackage;

import org.eclipse.emf.common.notify.NotificationChain;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.util.EObjectContainmentEList;
import org.eclipse.emf.ecore.util.InternalEList;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Platform Call Module</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.controller.core.model.platform_call.impl.PlatformCallModuleImpl#getPlatformCalls <em>Platform Calls</em>}</li>
 * </ul>
 *
 * @generated
 */
public class PlatformCallModuleImpl extends ModuleImpl implements PlatformCallModule
{
	/**
	 * The cached value of the '{@link #getPlatformCalls() <em>Platform Calls</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getPlatformCalls()
	 * @generated
	 * @ordered
	 */
	protected EList<PlatformCall> platformCalls;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected PlatformCallModuleImpl()
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
		return Platform_callPackage.Literals.PLATFORM_CALL_MODULE;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EList<PlatformCall> getPlatformCalls()
	{
		if (platformCalls == null)
		{
			platformCalls = new EObjectContainmentEList<PlatformCall>(PlatformCall.class, this, Platform_callPackage.PLATFORM_CALL_MODULE__PLATFORM_CALLS);
		}
		return platformCalls;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public NotificationChain eInverseRemove(InternalEObject otherEnd, int featureID, NotificationChain msgs)
	{
		switch (featureID)
		{
			case Platform_callPackage.PLATFORM_CALL_MODULE__PLATFORM_CALLS:
				return ((InternalEList<?>)getPlatformCalls()).basicRemove(otherEnd, msgs);
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
		switch (featureID)
		{
			case Platform_callPackage.PLATFORM_CALL_MODULE__PLATFORM_CALLS:
				return getPlatformCalls();
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
	public void eSet(int featureID, Object newValue)
	{
		switch (featureID)
		{
			case Platform_callPackage.PLATFORM_CALL_MODULE__PLATFORM_CALLS:
				getPlatformCalls().clear();
				getPlatformCalls().addAll((Collection<? extends PlatformCall>)newValue);
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
		switch (featureID)
		{
			case Platform_callPackage.PLATFORM_CALL_MODULE__PLATFORM_CALLS:
				getPlatformCalls().clear();
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
		switch (featureID)
		{
			case Platform_callPackage.PLATFORM_CALL_MODULE__PLATFORM_CALLS:
				return platformCalls != null && !platformCalls.isEmpty();
		}
		return super.eIsSet(featureID);
	}

} //PlatformCallModuleImpl
