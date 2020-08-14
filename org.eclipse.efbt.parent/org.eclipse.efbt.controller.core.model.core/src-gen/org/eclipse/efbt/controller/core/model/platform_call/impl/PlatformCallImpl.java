/**
 */
package org.eclipse.efbt.controller.core.model.platform_call.impl;

import org.eclipse.efbt.controller.core.model.platform_call.PlatformCall;
import org.eclipse.efbt.controller.core.model.platform_call.Platform_callPackage;

import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.ecore.EClass;

import org.eclipse.emf.ecore.impl.ENotificationImpl;
import org.eclipse.emf.ecore.impl.MinimalEObjectImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Platform Call</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.controller.core.model.platform_call.impl.PlatformCallImpl#getErrorMessage <em>Error Message</em>}</li>
 *   <li>{@link org.eclipse.efbt.controller.core.model.platform_call.impl.PlatformCallImpl#getName <em>Name</em>}</li>
 *   <li>{@link org.eclipse.efbt.controller.core.model.platform_call.impl.PlatformCallImpl#getReturnStatus <em>Return Status</em>}</li>
 * </ul>
 *
 * @generated
 */
public class PlatformCallImpl extends MinimalEObjectImpl.Container implements PlatformCall
{
	/**
	 * The default value of the '{@link #getErrorMessage() <em>Error Message</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getErrorMessage()
	 * @generated
	 * @ordered
	 */
	protected static final String ERROR_MESSAGE_EDEFAULT = null;

	/**
	 * The cached value of the '{@link #getErrorMessage() <em>Error Message</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getErrorMessage()
	 * @generated
	 * @ordered
	 */
	protected String errorMessage = ERROR_MESSAGE_EDEFAULT;

	/**
	 * The default value of the '{@link #getName() <em>Name</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getName()
	 * @generated
	 * @ordered
	 */
	protected static final String NAME_EDEFAULT = null;

	/**
	 * The cached value of the '{@link #getName() <em>Name</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getName()
	 * @generated
	 * @ordered
	 */
	protected String name = NAME_EDEFAULT;

	/**
	 * The default value of the '{@link #getReturnStatus() <em>Return Status</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getReturnStatus()
	 * @generated
	 * @ordered
	 */
	protected static final int RETURN_STATUS_EDEFAULT = 0;

	/**
	 * The cached value of the '{@link #getReturnStatus() <em>Return Status</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getReturnStatus()
	 * @generated
	 * @ordered
	 */
	protected int returnStatus = RETURN_STATUS_EDEFAULT;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected PlatformCallImpl()
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
		return Platform_callPackage.Literals.PLATFORM_CALL;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public String getErrorMessage()
	{
		return errorMessage;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setErrorMessage(String newErrorMessage)
	{
		String oldErrorMessage = errorMessage;
		errorMessage = newErrorMessage;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Platform_callPackage.PLATFORM_CALL__ERROR_MESSAGE, oldErrorMessage, errorMessage));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public String getName()
	{
		return name;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setName(String newName)
	{
		String oldName = name;
		name = newName;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Platform_callPackage.PLATFORM_CALL__NAME, oldName, name));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public int getReturnStatus()
	{
		return returnStatus;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setReturnStatus(int newReturnStatus)
	{
		int oldReturnStatus = returnStatus;
		returnStatus = newReturnStatus;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Platform_callPackage.PLATFORM_CALL__RETURN_STATUS, oldReturnStatus, returnStatus));
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
			case Platform_callPackage.PLATFORM_CALL__ERROR_MESSAGE:
				return getErrorMessage();
			case Platform_callPackage.PLATFORM_CALL__NAME:
				return getName();
			case Platform_callPackage.PLATFORM_CALL__RETURN_STATUS:
				return getReturnStatus();
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
		switch (featureID)
		{
			case Platform_callPackage.PLATFORM_CALL__ERROR_MESSAGE:
				setErrorMessage((String)newValue);
				return;
			case Platform_callPackage.PLATFORM_CALL__NAME:
				setName((String)newValue);
				return;
			case Platform_callPackage.PLATFORM_CALL__RETURN_STATUS:
				setReturnStatus((Integer)newValue);
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
			case Platform_callPackage.PLATFORM_CALL__ERROR_MESSAGE:
				setErrorMessage(ERROR_MESSAGE_EDEFAULT);
				return;
			case Platform_callPackage.PLATFORM_CALL__NAME:
				setName(NAME_EDEFAULT);
				return;
			case Platform_callPackage.PLATFORM_CALL__RETURN_STATUS:
				setReturnStatus(RETURN_STATUS_EDEFAULT);
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
			case Platform_callPackage.PLATFORM_CALL__ERROR_MESSAGE:
				return ERROR_MESSAGE_EDEFAULT == null ? errorMessage != null : !ERROR_MESSAGE_EDEFAULT.equals(errorMessage);
			case Platform_callPackage.PLATFORM_CALL__NAME:
				return NAME_EDEFAULT == null ? name != null : !NAME_EDEFAULT.equals(name);
			case Platform_callPackage.PLATFORM_CALL__RETURN_STATUS:
				return returnStatus != RETURN_STATUS_EDEFAULT;
		}
		return super.eIsSet(featureID);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public String toString()
	{
		if (eIsProxy()) return super.toString();

		StringBuilder result = new StringBuilder(super.toString());
		result.append(" (errorMessage: ");
		result.append(errorMessage);
		result.append(", name: ");
		result.append(name);
		result.append(", returnStatus: ");
		result.append(returnStatus);
		result.append(')');
		return result.toString();
	}

} //PlatformCallImpl
