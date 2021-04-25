/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl;

import org.eclipse.efbt.cocalimo.smcubes.model.core.SUBDOMAIN;

import org.eclipse.efbt.cocalimo.smcubes.model.data_definition.Data_definitionPackage;
import org.eclipse.efbt.cocalimo.smcubes.model.data_definition.FRAMEWORK;
import org.eclipse.efbt.cocalimo.smcubes.model.data_definition.FRAMEWORK_SUBDOMAIN;

import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;
import org.eclipse.emf.ecore.impl.MinimalEObjectImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>FRAMEWORK SUBDOMAIN</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.FRAMEWORK_SUBDOMAINImpl#getFramework_id <em>Framework id</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.FRAMEWORK_SUBDOMAINImpl#getSubdomain_id <em>Subdomain id</em>}</li>
 * </ul>
 *
 * @generated
 */
public class FRAMEWORK_SUBDOMAINImpl extends MinimalEObjectImpl.Container implements FRAMEWORK_SUBDOMAIN
{
	/**
	 * The cached value of the '{@link #getFramework_id() <em>Framework id</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getFramework_id()
	 * @generated
	 * @ordered
	 */
	protected FRAMEWORK framework_id;

	/**
	 * The cached value of the '{@link #getSubdomain_id() <em>Subdomain id</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getSubdomain_id()
	 * @generated
	 * @ordered
	 */
	protected SUBDOMAIN subdomain_id;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected FRAMEWORK_SUBDOMAINImpl()
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
		return Data_definitionPackage.Literals.FRAMEWORK_SUBDOMAIN;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public FRAMEWORK getFramework_id()
	{
		if (framework_id != null && framework_id.eIsProxy()) {
			InternalEObject oldFramework_id = (InternalEObject)framework_id;
			framework_id = (FRAMEWORK)eResolveProxy(oldFramework_id);
			if (framework_id != oldFramework_id) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Data_definitionPackage.FRAMEWORK_SUBDOMAIN__FRAMEWORK_ID, oldFramework_id, framework_id));
			}
		}
		return framework_id;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public FRAMEWORK basicGetFramework_id()
	{
		return framework_id;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setFramework_id(FRAMEWORK newFramework_id)
	{
		FRAMEWORK oldFramework_id = framework_id;
		framework_id = newFramework_id;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Data_definitionPackage.FRAMEWORK_SUBDOMAIN__FRAMEWORK_ID, oldFramework_id, framework_id));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public SUBDOMAIN getSubdomain_id()
	{
		if (subdomain_id != null && subdomain_id.eIsProxy()) {
			InternalEObject oldSubdomain_id = (InternalEObject)subdomain_id;
			subdomain_id = (SUBDOMAIN)eResolveProxy(oldSubdomain_id);
			if (subdomain_id != oldSubdomain_id) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Data_definitionPackage.FRAMEWORK_SUBDOMAIN__SUBDOMAIN_ID, oldSubdomain_id, subdomain_id));
			}
		}
		return subdomain_id;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public SUBDOMAIN basicGetSubdomain_id()
	{
		return subdomain_id;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setSubdomain_id(SUBDOMAIN newSubdomain_id)
	{
		SUBDOMAIN oldSubdomain_id = subdomain_id;
		subdomain_id = newSubdomain_id;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Data_definitionPackage.FRAMEWORK_SUBDOMAIN__SUBDOMAIN_ID, oldSubdomain_id, subdomain_id));
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
			case Data_definitionPackage.FRAMEWORK_SUBDOMAIN__FRAMEWORK_ID:
				if (resolve) return getFramework_id();
				return basicGetFramework_id();
			case Data_definitionPackage.FRAMEWORK_SUBDOMAIN__SUBDOMAIN_ID:
				if (resolve) return getSubdomain_id();
				return basicGetSubdomain_id();
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
			case Data_definitionPackage.FRAMEWORK_SUBDOMAIN__FRAMEWORK_ID:
				setFramework_id((FRAMEWORK)newValue);
				return;
			case Data_definitionPackage.FRAMEWORK_SUBDOMAIN__SUBDOMAIN_ID:
				setSubdomain_id((SUBDOMAIN)newValue);
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
			case Data_definitionPackage.FRAMEWORK_SUBDOMAIN__FRAMEWORK_ID:
				setFramework_id((FRAMEWORK)null);
				return;
			case Data_definitionPackage.FRAMEWORK_SUBDOMAIN__SUBDOMAIN_ID:
				setSubdomain_id((SUBDOMAIN)null);
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
			case Data_definitionPackage.FRAMEWORK_SUBDOMAIN__FRAMEWORK_ID:
				return framework_id != null;
			case Data_definitionPackage.FRAMEWORK_SUBDOMAIN__SUBDOMAIN_ID:
				return subdomain_id != null;
		}
		return super.eIsSet(featureID);
	}

} //FRAMEWORK_SUBDOMAINImpl
