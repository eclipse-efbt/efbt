/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extension.impl;

import org.eclipse.efbt.cocalimo.core.model.module_management.impl.ModuleImpl;

import org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extension.Cocalimo_smcubes_extensionPackage;
import org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extension.SubDomainEnumerationModule;

import org.eclipse.efbt.cocalimo.smcubes.model.core.SUBDOMAIN_ENUMERATION;

import org.eclipse.emf.common.notify.Notification;
import org.eclipse.emf.common.notify.NotificationChain;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Sub Domain Enumeration Module</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extension.impl.SubDomainEnumerationModuleImpl#getSubdomainEnumerations <em>Subdomain Enumerations</em>}</li>
 * </ul>
 *
 * @generated
 */
public class SubDomainEnumerationModuleImpl extends ModuleImpl implements SubDomainEnumerationModule {
	/**
	 * The cached value of the '{@link #getSubdomainEnumerations() <em>Subdomain Enumerations</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getSubdomainEnumerations()
	 * @generated
	 * @ordered
	 */
	protected SUBDOMAIN_ENUMERATION subdomainEnumerations;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected SubDomainEnumerationModuleImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return Cocalimo_smcubes_extensionPackage.Literals.SUB_DOMAIN_ENUMERATION_MODULE;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public SUBDOMAIN_ENUMERATION getSubdomainEnumerations() {
		return subdomainEnumerations;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public NotificationChain basicSetSubdomainEnumerations(SUBDOMAIN_ENUMERATION newSubdomainEnumerations, NotificationChain msgs) {
		SUBDOMAIN_ENUMERATION oldSubdomainEnumerations = subdomainEnumerations;
		subdomainEnumerations = newSubdomainEnumerations;
		if (eNotificationRequired()) {
			ENotificationImpl notification = new ENotificationImpl(this, Notification.SET, Cocalimo_smcubes_extensionPackage.SUB_DOMAIN_ENUMERATION_MODULE__SUBDOMAIN_ENUMERATIONS, oldSubdomainEnumerations, newSubdomainEnumerations);
			if (msgs == null) msgs = notification; else msgs.add(notification);
		}
		return msgs;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setSubdomainEnumerations(SUBDOMAIN_ENUMERATION newSubdomainEnumerations) {
		if (newSubdomainEnumerations != subdomainEnumerations) {
			NotificationChain msgs = null;
			if (subdomainEnumerations != null)
				msgs = ((InternalEObject)subdomainEnumerations).eInverseRemove(this, EOPPOSITE_FEATURE_BASE - Cocalimo_smcubes_extensionPackage.SUB_DOMAIN_ENUMERATION_MODULE__SUBDOMAIN_ENUMERATIONS, null, msgs);
			if (newSubdomainEnumerations != null)
				msgs = ((InternalEObject)newSubdomainEnumerations).eInverseAdd(this, EOPPOSITE_FEATURE_BASE - Cocalimo_smcubes_extensionPackage.SUB_DOMAIN_ENUMERATION_MODULE__SUBDOMAIN_ENUMERATIONS, null, msgs);
			msgs = basicSetSubdomainEnumerations(newSubdomainEnumerations, msgs);
			if (msgs != null) msgs.dispatch();
		}
		else if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Cocalimo_smcubes_extensionPackage.SUB_DOMAIN_ENUMERATION_MODULE__SUBDOMAIN_ENUMERATIONS, newSubdomainEnumerations, newSubdomainEnumerations));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public NotificationChain eInverseRemove(InternalEObject otherEnd, int featureID, NotificationChain msgs) {
		switch (featureID) {
			case Cocalimo_smcubes_extensionPackage.SUB_DOMAIN_ENUMERATION_MODULE__SUBDOMAIN_ENUMERATIONS:
				return basicSetSubdomainEnumerations(null, msgs);
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
			case Cocalimo_smcubes_extensionPackage.SUB_DOMAIN_ENUMERATION_MODULE__SUBDOMAIN_ENUMERATIONS:
				return getSubdomainEnumerations();
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
			case Cocalimo_smcubes_extensionPackage.SUB_DOMAIN_ENUMERATION_MODULE__SUBDOMAIN_ENUMERATIONS:
				setSubdomainEnumerations((SUBDOMAIN_ENUMERATION)newValue);
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
			case Cocalimo_smcubes_extensionPackage.SUB_DOMAIN_ENUMERATION_MODULE__SUBDOMAIN_ENUMERATIONS:
				setSubdomainEnumerations((SUBDOMAIN_ENUMERATION)null);
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
			case Cocalimo_smcubes_extensionPackage.SUB_DOMAIN_ENUMERATION_MODULE__SUBDOMAIN_ENUMERATIONS:
				return subdomainEnumerations != null;
		}
		return super.eIsSet(featureID);
	}

} //SubDomainEnumerationModuleImpl
