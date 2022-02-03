/**
 */
package org.eclipse.efbt.cocalimo.computation.model.ecore_plus_structure.impl;

import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_structure.Ecore_plus_structurePackage;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_structure.EnumeratedDomain;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_structure.SubDomainEnum;

import org.eclipse.efbt.cocalimo.smcubes.model.core.SUBDOMAIN;

import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Sub Domain Enum</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_structure.impl.SubDomainEnumImpl#getPrimaryEnum <em>Primary Enum</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_structure.impl.SubDomainEnumImpl#getSubDomain <em>Sub Domain</em>}</li>
 * </ul>
 *
 * @generated
 */
public class SubDomainEnumImpl extends HeirarchicalEnumImpl implements SubDomainEnum {
	/**
	 * The cached value of the '{@link #getPrimaryEnum() <em>Primary Enum</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getPrimaryEnum()
	 * @generated
	 * @ordered
	 */
	protected EnumeratedDomain primaryEnum;

	/**
	 * The cached value of the '{@link #getSubDomain() <em>Sub Domain</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getSubDomain()
	 * @generated
	 * @ordered
	 */
	protected SUBDOMAIN subDomain;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected SubDomainEnumImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return Ecore_plus_structurePackage.Literals.SUB_DOMAIN_ENUM;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EnumeratedDomain getPrimaryEnum() {
		if (primaryEnum != null && primaryEnum.eIsProxy()) {
			InternalEObject oldPrimaryEnum = (InternalEObject)primaryEnum;
			primaryEnum = (EnumeratedDomain)eResolveProxy(oldPrimaryEnum);
			if (primaryEnum != oldPrimaryEnum) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Ecore_plus_structurePackage.SUB_DOMAIN_ENUM__PRIMARY_ENUM, oldPrimaryEnum, primaryEnum));
			}
		}
		return primaryEnum;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EnumeratedDomain basicGetPrimaryEnum() {
		return primaryEnum;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setPrimaryEnum(EnumeratedDomain newPrimaryEnum) {
		EnumeratedDomain oldPrimaryEnum = primaryEnum;
		primaryEnum = newPrimaryEnum;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Ecore_plus_structurePackage.SUB_DOMAIN_ENUM__PRIMARY_ENUM, oldPrimaryEnum, primaryEnum));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public SUBDOMAIN getSubDomain() {
		if (subDomain != null && subDomain.eIsProxy()) {
			InternalEObject oldSubDomain = (InternalEObject)subDomain;
			subDomain = (SUBDOMAIN)eResolveProxy(oldSubDomain);
			if (subDomain != oldSubDomain) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Ecore_plus_structurePackage.SUB_DOMAIN_ENUM__SUB_DOMAIN, oldSubDomain, subDomain));
			}
		}
		return subDomain;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public SUBDOMAIN basicGetSubDomain() {
		return subDomain;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setSubDomain(SUBDOMAIN newSubDomain) {
		SUBDOMAIN oldSubDomain = subDomain;
		subDomain = newSubDomain;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Ecore_plus_structurePackage.SUB_DOMAIN_ENUM__SUB_DOMAIN, oldSubDomain, subDomain));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object eGet(int featureID, boolean resolve, boolean coreType) {
		switch (featureID) {
			case Ecore_plus_structurePackage.SUB_DOMAIN_ENUM__PRIMARY_ENUM:
				if (resolve) return getPrimaryEnum();
				return basicGetPrimaryEnum();
			case Ecore_plus_structurePackage.SUB_DOMAIN_ENUM__SUB_DOMAIN:
				if (resolve) return getSubDomain();
				return basicGetSubDomain();
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
			case Ecore_plus_structurePackage.SUB_DOMAIN_ENUM__PRIMARY_ENUM:
				setPrimaryEnum((EnumeratedDomain)newValue);
				return;
			case Ecore_plus_structurePackage.SUB_DOMAIN_ENUM__SUB_DOMAIN:
				setSubDomain((SUBDOMAIN)newValue);
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
			case Ecore_plus_structurePackage.SUB_DOMAIN_ENUM__PRIMARY_ENUM:
				setPrimaryEnum((EnumeratedDomain)null);
				return;
			case Ecore_plus_structurePackage.SUB_DOMAIN_ENUM__SUB_DOMAIN:
				setSubDomain((SUBDOMAIN)null);
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
			case Ecore_plus_structurePackage.SUB_DOMAIN_ENUM__PRIMARY_ENUM:
				return primaryEnum != null;
			case Ecore_plus_structurePackage.SUB_DOMAIN_ENUM__SUB_DOMAIN:
				return subDomain != null;
		}
		return super.eIsSet(featureID);
	}

} //SubDomainEnumImpl
